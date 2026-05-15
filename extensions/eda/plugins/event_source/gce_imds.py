"""
gce_imds.py — ansible-rulebook event source plugin.

Watches the GCE-compatible instance metadata service (IMDS) at
http://169.254.169.254/computeMetadata/v1/ and emits an event when the value
of one of the configured `watch:` keys changes.

Supported backends:
  * Google Compute Engine — native wait_for_change long-poll.
  * Yandex.Cloud         — fully GCE-compatible; supports wait_for_change.
  * Anything else with a compatible /computeMetadata/v1/ tree — degrades
    automatically to interval polling with ETag-based dedup.

The plugin can extract a full playbook YAML from a metadata key, write it to a
local cache directory, and pass the path on the event, so a rulebook can dispatch
`run_playbook` against that path. Other keys can be emitted as plain
extra_vars or as URL references to be fetched by the playbook itself.

Event payload schema:
    {
      "trigger":       bool,         # this key fires a playbook run
      "key":           str,          # metadata key that changed
      "kind":          str,          # "inline" | "ref" | "extra_vars"
      "old_value":     Any | None,
      "new_value":     Any,
      "etag":          str | None,
      "removed":       bool,         # only when value disappeared
      "playbook_path": str,          # only when kind=inline and triggers=true
      "playbook_url":  str,          # only when kind=ref and triggers=true
      "extra_vars":    dict,         # only when kind=extra_vars
      "meta": {
        "instance_id": str | None,
        "hostname":    str | None,
        "ts":          float,
        "source":      "gce_imds",
      }
    }
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

import aiohttp
import yaml

DOCUMENTATION = r"""
---
short_description: Watch a GCE-compatible IMDS for VM metadata changes.
description:
  - Connects to the local instance metadata service at 169.254.169.254 and watches
    a configured set of keys under instance/attributes/.
  - When a watched key changes, the plugin emits an event. For keys marked
    triggers=true and kind=inline, the value is treated as a full Ansible playbook
    in YAML and cached to a local file; the path is included in the event so the
    rulebook can launch it directly with `run_playbook`.
  - Uses wait_for_change long-poll when the IMDS supports it (GCE, Yandex.Cloud);
    transparently falls back to interval polling when not supported.
  - Persists the last-seen ETag to a state file so that restarting the plugin
    does not re-emit unchanged values.
options:
  endpoint:
    description: Base URL of the metadata service.
    type: str
    default: http://169.254.169.254/computeMetadata/v1
  mode:
    description: >
      auto: probe the IMDS once for wait_for_change support, then commit to that mode.
      wait_for_change: long-poll only (fails loudly if unsupported).
      poll: interval polling only.
    type: str
    choices: [auto, wait_for_change, poll]
    default: auto
  timeout_sec:
    description: Hang time for wait_for_change requests, in seconds.
    type: int
    default: 300
  poll_interval:
    description: Sleep between polls in poll mode, in seconds.
    type: int
    default: 30
  state_file:
    description: Path to a JSON file that persists last seen ETag and data.
    type: str
    default: /var/lib/ansible-rulebook/imds-state.json
  playbook_cache_dir:
    description: Directory where inline playbook YAML is materialized as files.
    type: str
    default: /var/lib/ansible-rulebook/playbooks
  initial:
    description: >
      Startup behavior. ignore: do nothing until next change.
      diff_with_state: compare current ETag with the saved one and emit if different.
      always: force-emit current state on every start.
    type: str
    choices: [ignore, diff_with_state, always]
    default: diff_with_state
  watch:
    description: List of metadata keys to monitor.
    type: list
    elements: dict
    required: true
    suboptions:
      key:
        description: Key name under instance/attributes/. Slash-separated paths
          are supported for nested values (recursive=true is always on).
        type: str
        required: true
      kind:
        description: >
          inline    — value is a YAML playbook; gets cached and event.playbook_path is set.
          ref       — value is a URL; event.playbook_url is set and the playbook fetches it.
          extra_vars — value is treated as YAML/JSON dict (or scalar) and put into event.extra_vars.
        type: str
        choices: [inline, ref, extra_vars]
        default: inline
      triggers:
        description: If true, change of this key fires a run_playbook event.
        type: bool
        default: false
"""

EXAMPLES = r"""
- name: vm-metadata-driven automation
  hosts: localhost
  sources:
    - valerii.metadata.gce_imds:
        mode: auto
        watch:
          - key: ansible-playbook
            kind: inline
            triggers: true
          - key: ansible-extra-vars
            kind: extra_vars
  rules:
    - name: run playbook from metadata
      condition: event.trigger == true and event.playbook_path is defined
      throttle:
        group_by_attributes:
          - event.meta.instance_id
        within: 1 minute
      action:
        run_playbook:
          name: "{{ event.playbook_path }}"
          extra_vars: "{{ event.extra_vars | default({}) }}"
"""

LOGGER = logging.getLogger("eda.source.gce_imds")

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
DEFAULT_ENDPOINT = "http://169.254.169.254/computeMetadata/v1"
DEFAULT_TIMEOUT_SEC = 300
DEFAULT_POLL_INTERVAL = 30
DEFAULT_STATE_FILE = "/var/lib/ansible-rulebook/imds-state.json"
DEFAULT_PLAYBOOK_CACHE = "/var/lib/ansible-rulebook/playbooks"

VALID_MODES = ("auto", "wait_for_change", "poll")
VALID_INITIAL = ("ignore", "diff_with_state", "always")
VALID_KINDS = ("inline", "ref", "extra_vars")

REQUIRED_HEADERS = {"Metadata-Flavor": "Google"}


# --------------------------------------------------------------------------- #
# Entrypoint required by ansible-rulebook
# --------------------------------------------------------------------------- #
async def main(queue: asyncio.Queue, args: dict[str, Any]) -> None:
    """ansible-rulebook source plugin entrypoint."""
    config = _validate_config(args)
    LOGGER.info(
        "starting gce_imds: mode=%s endpoint=%s watch=%s",
        config["mode"], config["endpoint"],
        [w["key"] for w in config["watch"]],
    )
    state = _load_state(config["state_file"])

    async with aiohttp.ClientSession(
        headers=REQUIRED_HEADERS,
        timeout=aiohttp.ClientTimeout(
            total=None,
            sock_read=config["timeout_sec"] + 30,
            sock_connect=10,
        ),
    ) as session:
        instance_info = await _fetch_instance_info(session, config["endpoint"])

        if config["initial"] != "ignore":
            await _check_initial_state(queue, session, config, state, instance_info)

        active_mode = config["mode"]
        if active_mode == "auto":
            supported = await _probe_wait_for_change(session, config)
            active_mode = "wait_for_change" if supported else "poll"
            LOGGER.info("auto-detected mode=%s", active_mode)

        if active_mode == "wait_for_change":
            await _watch_long_poll(queue, session, config, state, instance_info)
        else:
            await _watch_interval_poll(queue, session, config, state, instance_info)


# --------------------------------------------------------------------------- #
# Long-poll loop (wait_for_change)
# --------------------------------------------------------------------------- #
async def _watch_long_poll(queue, session, config, state, instance_info) -> None:
    url = f"{config['endpoint']}/instance/attributes/"
    backoff = 1.0

    while True:
        params = {
            "recursive": "true",
            "alt": "json",
            "wait_for_change": "true",
            "timeout_sec": str(config["timeout_sec"]),
        }
        last_etag = state.get("last_etag")
        if last_etag:
            params["last_etag"] = last_etag

        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    backoff = 1.0
                    new_etag = resp.headers.get("ETag")
                    if new_etag and new_etag == state.get("last_etag"):
                        # Server returned without actual change (timeout). Continue.
                        continue
                    new_data = await _parse_attributes_response(resp)
                    await _handle_change(queue, config, state, new_data, new_etag, instance_info)
                elif resp.status in (400, 404):
                    LOGGER.warning(
                        "wait_for_change rejected (status=%s); switching permanently to poll",
                        resp.status,
                    )
                    await _watch_interval_poll(queue, session, config, state, instance_info)
                    return
                else:
                    LOGGER.warning("IMDS unexpected status=%s body=%s",
                                   resp.status, await resp.text())
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            LOGGER.warning("IMDS long-poll failed: %s (retry in %.1fs)", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# --------------------------------------------------------------------------- #
# Interval-poll loop (fallback)
# --------------------------------------------------------------------------- #
async def _watch_interval_poll(queue, session, config, state, instance_info) -> None:
    url = f"{config['endpoint']}/instance/attributes/"
    interval = config["poll_interval"]

    while True:
        try:
            async with session.get(url, params={"recursive": "true", "alt": "json"}) as resp:
                if resp.status == 200:
                    new_etag = resp.headers.get("ETag")
                    if new_etag and new_etag == state.get("last_etag"):
                        pass  # no change
                    else:
                        new_data = await _parse_attributes_response(resp)
                        await _handle_change(queue, config, state, new_data, new_etag, instance_info)
                else:
                    LOGGER.warning("IMDS poll status=%s", resp.status)
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            LOGGER.warning("IMDS poll error: %s", e)

        await asyncio.sleep(interval)


# --------------------------------------------------------------------------- #
# Change handler — figure out which watched keys changed, emit events
# --------------------------------------------------------------------------- #
async def _handle_change(queue, config, state, new_data, new_etag, instance_info) -> None:
    old_data = state.get("data") or {}
    emitted = 0

    for entry in config["watch"]:
        key = entry["key"]
        new_val = _lookup_key(new_data, key)
        old_val = _lookup_key(old_data, key)

        if new_val == old_val:
            continue

        if new_val is None:
            # Key was removed. Emit for visibility but never auto-trigger on removal.
            ev = _base_event(entry, key, old_val, new_val, new_etag, instance_info)
            ev["trigger"] = False
            ev["removed"] = True
            await queue.put(ev)
            emitted += 1
            continue

        ev = await _build_event(config, entry, key, old_val, new_val, new_etag, instance_info)
        if ev is not None:
            await queue.put(ev)
            emitted += 1

    LOGGER.info("change processed etag=%s emitted=%d", new_etag, emitted)
    state["last_etag"] = new_etag
    state["data"] = new_data
    _save_state(config["state_file"], state)


async def _build_event(config, entry, key, old_val, new_val, etag, instance_info) -> dict | None:
    kind = entry.get("kind", "inline")
    triggers = bool(entry.get("triggers", False))
    ev = _base_event(entry, key, old_val, new_val, etag, instance_info)
    ev["trigger"] = triggers

    if kind == "inline":
        if not triggers:
            return ev  # contextual only
        if not isinstance(new_val, str):
            LOGGER.error("key=%s kind=inline but value is not a string", key)
            return None
        path = _cache_playbook(config["playbook_cache_dir"], new_val)
        if path is None:
            return None
        ev["playbook_path"] = str(path)

    elif kind == "ref":
        if triggers and not isinstance(new_val, str):
            LOGGER.error("key=%s kind=ref but value is not a URL string", key)
            return None
        ev["playbook_url"] = new_val

    elif kind == "extra_vars":
        parsed: Any = new_val
        if isinstance(new_val, str):
            try:
                parsed = yaml.safe_load(new_val)
            except yaml.YAMLError:
                parsed = new_val
        ev["extra_vars"] = parsed if isinstance(parsed, dict) else {"value": parsed}

    return ev


def _base_event(entry, key, old_val, new_val, etag, instance_info) -> dict:
    return {
        "key": key,
        "kind": entry.get("kind", "inline"),
        "old_value": old_val,
        "new_value": new_val,
        "etag": etag,
        "meta": {
            "instance_id": instance_info.get("id"),
            "hostname": instance_info.get("hostname"),
            "ts": time.time(),
            "source": "gce_imds",
        },
    }


def _cache_playbook(cache_dir: str, yaml_text: str) -> Path | None:
    """Validate YAML structure and atomically write to cache_dir/<sha>.yml."""
    try:
        parsed = yaml.safe_load(yaml_text)
    except yaml.YAMLError as e:
        LOGGER.error("invalid playbook YAML: %s", e)
        return None
    if not isinstance(parsed, list):
        LOGGER.error("playbook must be a YAML list of plays, got %s",
                     type(parsed).__name__)
        return None

    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(yaml_text.encode("utf-8")).hexdigest()[:16]
    target = cache / f"{digest}.yml"
    if target.exists():
        return target
    tmp = cache / f".{digest}.yml.tmp"
    tmp.write_text(yaml_text, encoding="utf-8")
    tmp.replace(target)
    return target


# --------------------------------------------------------------------------- #
# IMDS helpers
# --------------------------------------------------------------------------- #
def _lookup_key(data: dict | None, key: str) -> Any:
    """Lookup possibly nested key. Slash-separated walks dicts."""
    if data is None:
        return None
    if "/" not in key:
        return data.get(key) if isinstance(data, dict) else None
    cur: Any = data
    for part in key.split("/"):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


async def _parse_attributes_response(resp: aiohttp.ClientResponse) -> dict:
    text = await resp.text()
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {"_raw": data}
    except json.JSONDecodeError:
        return {"_raw": text}


async def _fetch_instance_info(session: aiohttp.ClientSession, endpoint: str) -> dict:
    """Best-effort fetch of instance id and hostname for event enrichment."""
    info: dict[str, Any] = {}
    for field, path in (("id", "instance/id"), ("hostname", "instance/hostname")):
        try:
            async with session.get(
                f"{endpoint}/{path}",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                if r.status == 200:
                    info[field] = (await r.text()).strip()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            LOGGER.debug("instance info fetch %s failed: %s", path, e)
    return info


async def _probe_wait_for_change(session: aiohttp.ClientSession, config: dict) -> bool:
    url = f"{config['endpoint']}/instance/attributes/"
    try:
        async with session.get(
            url,
            params={
                "recursive": "true",
                "alt": "json",
                "wait_for_change": "true",
                "timeout_sec": "1",
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return r.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        LOGGER.debug("wait_for_change probe failed: %s", e)
        return False


async def _check_initial_state(queue, session, config, state, instance_info) -> None:
    if config["initial"] == "always":
        state["last_etag"] = None
        state["data"] = {}

    url = f"{config['endpoint']}/instance/attributes/"
    try:
        async with session.get(
            url,
            params={"recursive": "true", "alt": "json"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            if r.status != 200:
                LOGGER.warning("initial state fetch status=%s", r.status)
                return
            new_etag = r.headers.get("ETag")
            if new_etag and new_etag == state.get("last_etag"):
                LOGGER.info("initial state matches saved ETag, no diff")
                return
            new_data = await _parse_attributes_response(r)
            LOGGER.info(
                "initial state differs (saved_etag=%s new_etag=%s)",
                state.get("last_etag"), new_etag,
            )
            await _handle_change(queue, config, state, new_data, new_etag, instance_info)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        LOGGER.warning("initial state check failed: %s", e)


# --------------------------------------------------------------------------- #
# State persistence (atomic)
# --------------------------------------------------------------------------- #
def _load_state(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        LOGGER.warning("failed to load state file %s: %s", path, e)
        return {}


def _save_state(path: str, state: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state), encoding="utf-8")
    tmp.replace(p)


# --------------------------------------------------------------------------- #
# Config validation
# --------------------------------------------------------------------------- #
def _validate_config(args: dict) -> dict:
    if not isinstance(args, dict):
        raise ValueError("source args must be a mapping")

    mode = args.get("mode", "auto")
    if mode not in VALID_MODES:
        raise ValueError(f"mode must be one of {VALID_MODES}, got {mode!r}")

    initial = args.get("initial", "diff_with_state")
    if initial not in VALID_INITIAL:
        raise ValueError(f"initial must be one of {VALID_INITIAL}, got {initial!r}")

    watch_raw = args.get("watch") or []
    if not isinstance(watch_raw, list) or not watch_raw:
        raise ValueError("watch must be a non-empty list of {key, kind, triggers} entries")

    watch: list[dict] = []
    for i, raw in enumerate(watch_raw):
        if not isinstance(raw, dict) or "key" not in raw:
            raise ValueError(f"watch[{i}] must be a mapping with a 'key' field")
        kind = raw.get("kind", "inline")
        if kind not in VALID_KINDS:
            raise ValueError(f"watch[{i}].kind must be one of {VALID_KINDS}")
        watch.append({
            "key": raw["key"],
            "kind": kind,
            "triggers": bool(raw.get("triggers", False)),
        })

    return {
        "endpoint": args.get("endpoint", DEFAULT_ENDPOINT).rstrip("/"),
        "mode": mode,
        "timeout_sec": int(args.get("timeout_sec", DEFAULT_TIMEOUT_SEC)),
        "poll_interval": int(args.get("poll_interval", DEFAULT_POLL_INTERVAL)),
        "state_file": args.get("state_file", DEFAULT_STATE_FILE),
        "playbook_cache_dir": args.get("playbook_cache_dir", DEFAULT_PLAYBOOK_CACHE),
        "initial": initial,
        "watch": watch,
    }


# --------------------------------------------------------------------------- #
# Standalone testing harness: `python gce_imds.py` runs the plugin against
# a real or mocked IMDS and prints events to stdout. Used in tests.
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if len(sys.argv) < 2:
        print("usage: gce_imds.py <args.yaml>", file=sys.stderr)
        sys.exit(2)

    with open(sys.argv[1]) as f:
        args_cli = yaml.safe_load(f)

    class _StdoutQueue:
        async def put(self, item):
            print(json.dumps(item, default=str, ensure_ascii=False))

    asyncio.run(main(_StdoutQueue(), args_cli))  # type: ignore[arg-type]
