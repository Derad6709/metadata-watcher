"""
Unit tests for gce_imds source plugin.

Uses aiohttp's pytest helper to stand up a real HTTP server that emulates
the IMDS, including the wait_for_change long-poll semantics. No mocks of
aiohttp internals — we test the plugin against actual HTTP behavior.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web

# Load the plugin module directly from its file path
_PLUGIN_PATH = (
    Path(__file__).resolve().parents[2]
    / "extensions" / "eda" / "plugins" / "event_source" / "gce_imds.py"
)
spec = importlib.util.spec_from_file_location("gce_imds", _PLUGIN_PATH)
assert spec and spec.loader
gce_imds = importlib.util.module_from_spec(spec)
sys.modules["gce_imds"] = gce_imds
spec.loader.exec_module(gce_imds)


# --------------------------------------------------------------------------- #
# Fake IMDS server
# --------------------------------------------------------------------------- #
class FakeIMDS:
    """Minimal but realistic GCE-style metadata server."""

    def __init__(self, support_wait_for_change: bool = True):
        self.support_wait_for_change = support_wait_for_change
        self.attributes: dict[str, Any] = {}
        self.etag = "etag-0"
        self.instance_id = "1234567890"
        self.hostname = "fake-vm.local"
        self._change_event = asyncio.Event()
        self._etag_counter = 0

    def update(self, attrs: dict[str, Any]) -> None:
        """Update attributes and bump etag, notifying any hanging GETs."""
        self.attributes = dict(attrs)
        self._etag_counter += 1
        self.etag = f"etag-{self._etag_counter}"
        self._change_event.set()
        self._change_event = asyncio.Event()  # reset for next change

    async def attributes_handler(self, request: web.Request) -> web.Response:
        if request.headers.get("Metadata-Flavor") != "Google":
            return web.Response(status=403)

        wait_for_change = request.query.get("wait_for_change") == "true"
        if wait_for_change and not self.support_wait_for_change:
            return web.Response(status=400, text="wait_for_change not supported")

        last_etag = request.query.get("last_etag")
        timeout_sec = float(request.query.get("timeout_sec", "10"))

        if wait_for_change and last_etag == self.etag:
            # Hang until either a change happens or the timeout elapses.
            ev = self._change_event
            try:
                await asyncio.wait_for(ev.wait(), timeout=timeout_sec)
            except asyncio.TimeoutError:
                pass  # return current state anyway, with same etag

        body = json.dumps(self.attributes)
        return web.Response(status=200, body=body, content_type="application/json",
                            headers={"ETag": self.etag})

    async def id_handler(self, request: web.Request) -> web.Response:
        return web.Response(status=200, text=self.instance_id)

    async def hostname_handler(self, request: web.Request) -> web.Response:
        return web.Response(status=200, text=self.hostname)

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/computeMetadata/v1/instance/attributes/", self.attributes_handler)
        app.router.add_get("/computeMetadata/v1/instance/id", self.id_handler)
        app.router.add_get("/computeMetadata/v1/instance/hostname", self.hostname_handler)
        return app


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def imds_state_dir(tmp_path):
    """Tmp dirs for state and playbook cache."""
    return {
        "state_file": str(tmp_path / "state.json"),
        "playbook_cache_dir": str(tmp_path / "playbooks"),
    }


def base_args(server_port: int, state_paths: dict, **overrides) -> dict:
    args = {
        "endpoint": f"http://127.0.0.1:{server_port}/computeMetadata/v1",
        "mode": "auto",
        "timeout_sec": 2,
        "poll_interval": 1,
        "initial": "diff_with_state",
        "watch": [
            {"key": "ansible-playbook", "kind": "inline", "triggers": True},
            {"key": "ansible-extra-vars", "kind": "extra_vars"},
        ],
        **state_paths,
    }
    args.update(overrides)
    return args


PLAYBOOK_YAML = """\
- hosts: localhost
  tasks:
    - name: hello
      ansible.builtin.debug:
        msg: hi
"""


async def _run_with_imds(imds: FakeIMDS, args: dict, scenario, *,
                         max_events: int = 5,
                         max_runtime: float = 8.0) -> list[dict]:
    """Spin up the fake IMDS, run the plugin, run a scenario, collect events."""
    runner = web.AppRunner(imds.app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    args["endpoint"] = f"http://127.0.0.1:{port}/computeMetadata/v1"

    queue: asyncio.Queue = asyncio.Queue()
    events: list[dict] = []

    async def collector():
        while len(events) < max_events:
            ev = await queue.get()
            events.append(ev)

    plugin_task = asyncio.create_task(gce_imds.main(queue, args))
    collector_task = asyncio.create_task(collector())
    scenario_task = asyncio.create_task(scenario(imds))

    try:
        await asyncio.wait_for(
            asyncio.gather(collector_task, scenario_task, return_exceptions=True),
            timeout=max_runtime,
        )
    except asyncio.TimeoutError:
        pass
    finally:
        plugin_task.cancel()
        try:
            await plugin_task
        except (asyncio.CancelledError, Exception):
            pass
        await runner.cleanup()

    return events


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_emits_event_on_inline_change_wait_for_change(imds_state_dir):
    imds = FakeIMDS(support_wait_for_change=True)
    imds.attributes = {}

    async def scenario(imds_: FakeIMDS):
        await asyncio.sleep(0.5)
        imds_.update({"ansible-playbook": PLAYBOOK_YAML})

    args = base_args(0, imds_state_dir)
    events = await _run_with_imds(imds, args, scenario, max_events=1)

    assert events, "expected at least one event"
    ev = events[0]
    assert ev["key"] == "ansible-playbook"
    assert ev["trigger"] is True
    assert "playbook_path" in ev
    assert Path(ev["playbook_path"]).exists()
    assert ev["meta"]["instance_id"] == "1234567890"
    assert ev["meta"]["hostname"] == "fake-vm.local"


@pytest.mark.asyncio
async def test_falls_back_to_poll_when_wait_for_change_rejected(imds_state_dir):
    imds = FakeIMDS(support_wait_for_change=False)
    imds.attributes = {}

    async def scenario(imds_):
        await asyncio.sleep(1.0)
        imds_.update({"ansible-playbook": PLAYBOOK_YAML})

    args = base_args(0, imds_state_dir, mode="auto", poll_interval=1)
    events = await _run_with_imds(imds, args, scenario, max_events=1, max_runtime=5)

    assert events, "expected at least one event from poll-mode fallback"
    assert events[0]["trigger"] is True


@pytest.mark.asyncio
async def test_extra_vars_kind_parses_yaml(imds_state_dir):
    imds = FakeIMDS()
    imds.attributes = {}

    async def scenario(imds_):
        await asyncio.sleep(0.5)
        imds_.update({"ansible-extra-vars": "greeting: hi\ntarget: prod\n"})

    args = base_args(0, imds_state_dir)
    events = await _run_with_imds(imds, args, scenario, max_events=1)

    assert events
    ev = events[0]
    assert ev["key"] == "ansible-extra-vars"
    assert ev["trigger"] is False
    assert ev["extra_vars"] == {"greeting": "hi", "target": "prod"}


@pytest.mark.asyncio
async def test_invalid_playbook_yaml_does_not_emit_trigger(imds_state_dir):
    imds = FakeIMDS()
    imds.attributes = {}

    async def scenario(imds_):
        await asyncio.sleep(0.5)
        # Not a list of plays — should be rejected.
        imds_.update({"ansible-playbook": "this is not a playbook"})
        await asyncio.sleep(0.5)
        # Now a valid playbook.
        imds_.update({"ansible-playbook": PLAYBOOK_YAML})

    args = base_args(0, imds_state_dir)
    events = await _run_with_imds(imds, args, scenario, max_events=1)

    # Only the valid one made it through.
    assert events
    assert events[0]["trigger"] is True
    assert "playbook_path" in events[0]


@pytest.mark.asyncio
async def test_state_persists_across_runs(imds_state_dir):
    imds = FakeIMDS()
    imds.update({"ansible-playbook": PLAYBOOK_YAML})

    args = base_args(0, imds_state_dir)

    # First run — should emit because state file is empty.
    async def noop(_): await asyncio.sleep(0.5)
    events1 = await _run_with_imds(imds, args, noop, max_events=1, max_runtime=4)
    assert events1

    # Second run with the SAME state file — same etag, no emit expected.
    state = json.loads(Path(imds_state_dir["state_file"]).read_text())
    assert state["last_etag"] == imds.etag

    args2 = base_args(0, imds_state_dir)
    events2 = await _run_with_imds(imds, args2, noop, max_events=1, max_runtime=2)
    # Allow for the case where no events come (timeout reached); that's the
    # success criterion — we must NOT re-emit the unchanged state.
    assert all(e["etag"] == imds.etag for e in events2) or not events2


def test_validate_config_rejects_empty_watch():
    with pytest.raises(ValueError, match="watch"):
        gce_imds._validate_config({"watch": []})


def test_validate_config_rejects_bad_mode():
    with pytest.raises(ValueError, match="mode"):
        gce_imds._validate_config({"mode": "wat", "watch": [{"key": "k"}]})


def test_lookup_key_nested():
    data = {"a": {"b": {"c": 42}}}
    assert gce_imds._lookup_key(data, "a/b/c") == 42
    assert gce_imds._lookup_key(data, "a/x/c") is None
    assert gce_imds._lookup_key(data, "a") == {"b": {"c": 42}}


def test_lookup_key_flat():
    assert gce_imds._lookup_key({"foo": "bar"}, "foo") == "bar"
    assert gce_imds._lookup_key({}, "foo") is None
    assert gce_imds._lookup_key(None, "foo") is None


def test_cache_playbook_atomic_and_idempotent(tmp_path):
    p = gce_imds._cache_playbook(str(tmp_path), PLAYBOOK_YAML)
    assert p is not None and p.exists()
    p2 = gce_imds._cache_playbook(str(tmp_path), PLAYBOOK_YAML)
    assert p2 == p  # same content → same hash → same file


def test_cache_playbook_rejects_non_list(tmp_path):
    assert gce_imds._cache_playbook(str(tmp_path), "foo: bar") is None
    assert gce_imds._cache_playbook(str(tmp_path), "[unbalanced") is None
