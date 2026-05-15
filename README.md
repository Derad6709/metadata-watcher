# metadata-watcher

Event-Driven Ansible source-плагин: запускает плейбуки при изменении пользовательской метадаты ВМ через GCE-совместимый IMDS.

Работает там, где у провайдера есть `wait_for_change`-long-poll (Yandex.Cloud, GCP), и автоматически деградирует до интервального опроса там, где нет (любой кастомный IMDS, on-prem с `gce-metadata-server`, и т.п.).

## Идея

ВМ объявляет, какой плейбук должен на ней гоняться, **прямо в собственной метадате**. Плагин висит на локальном IMDS, при изменении нужного ключа извлекает YAML, материализует его на диск и эмитит event в `ansible-rulebook`. Стандартный action `run_playbook` подбирает путь из event'а и исполняет.

## Установка

```bash
ansible-galaxy collection install derad6709.metadata-watcher
# или прямо из тарбола после `ansible-galaxy collection build`:
ansible-galaxy collection install derad6709-metadata-watcher-0.1.0.tar.gz
```

Зависимости плагина (`aiohttp`, `pyyaml`) уже стоят как транзитивные у `ansible-rulebook`.

## Использование

### 1. Объяви плейбук в метадате ВМ

```bash
cat > /tmp/play.yml <<EOF
- hosts: localhost
  connection: local
  tasks:
    - name: triggered from metadata
      ansible.builtin.debug:
        msg: "etag={{ metadata_change.etag }}"
EOF

yc compute instance add-metadata my-vm \
    --metadata-from-file ansible-playbook=/tmp/play.yml
```

GCP — то же самое через `gcloud compute instances add-metadata`.

### 2. Заведи рулбук

```yaml
- name: vm-metadata-driven automation
  hosts: localhost
  sources:
    - derad6709.metadata-watcher.gce_imds:
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
        group_by_attributes: [event.meta.instance_id]
        within: 1 minute
      action:
        run_playbook:
          name: "{{ event.playbook_path }}"
          extra_vars: "{{ event.extra_vars | default({}) }}"
```

### 3. Запусти

```bash
ansible-rulebook --rulebook rulebook.yml -i localhost,
```

При следующем `yc compute instance add-metadata ...` плейбук стартанёт автоматически.

## Параметры плагина

| ключ | default | назначение |
|---|---|---|
| `endpoint` | `http://169.254.169.254/computeMetadata/v1` | базовый URL IMDS |
| `mode` | `auto` | `auto` / `wait_for_change` / `poll` |
| `timeout_sec` | `300` | hang-time для long-poll |
| `poll_interval` | `30` | интервал опроса в `poll`-режиме (сек) |
| `state_file` | `/var/lib/ansible-rulebook/imds-state.json` | где хранится last ETag |
| `playbook_cache_dir` | `/var/lib/ansible-rulebook/playbooks` | куда плагин кладёт YAML из метадаты |
| `initial` | `diff_with_state` | `ignore` / `diff_with_state` / `always` |
| `watch` | — | список наблюдаемых ключей |

Каждая запись в `watch`:

| ключ | значение |
|---|---|
| `key` | имя ключа в `instance/attributes/` (slash-path для вложенных при `recursive=true`) |
| `kind` | `inline` (YAML плейбука), `ref` (URL), `extra_vars` (YAML-дикт) |
| `triggers` | `true` если изменение этого ключа должно запускать плейбук |

## Схема event'а

```python
{
  "trigger":       bool,     # совпадает с watch[].triggers
  "key":           str,
  "kind":          str,
  "old_value":     Any,
  "new_value":     Any,
  "etag":          str,
  "removed":       bool,     # только когда ключ удалили
  "playbook_path": str,      # только при kind=inline + triggers=true
  "playbook_url":  str,      # только при kind=ref + triggers=true
  "extra_vars":    dict,     # только при kind=extra_vars
  "meta": {
    "instance_id": str,
    "hostname":    str,
    "ts":          float,
    "source":      "gce_imds",
  }
}
```

## Поведение при рестарте

`initial: diff_with_state` (по умолчанию): при старте плагин читает текущее состояние IMDS, сравнивает ETag со state-файлом и эмитит event только если значения отличаются от того, что было записано на диск перед прошлой остановкой. Это даёт «exactly-once-on-change» семантику между рестартами.

`initial: ignore` — реагировать только на изменения после старта (новые ETag). `initial: always` — всегда эмитить текущее состояние при старте (для отладки или принудительного reapply).

## Конкуррентность

Плагин сам ничего не сериализует — он просто кладёт events в очередь `ansible-rulebook`. Queue-семантика реализуется на уровне правила через нативный `throttle:`. В примере выше события сгруппированы по `instance_id`, в пределах окна 1 минута выполнится только один плейбук — следующий встанет в очередь движка.

## Ограничения

* Размер метадаты: у GCE/Yandex 32 KB на ключ, 256 KB суммарно. Для крупных плейбуков с ролями используй `kind: ref` и тяни YAML по URL внутри самого плейбука (через `ansible.builtin.uri` или `ansible.builtin.get_url`).
* IMDS доступен только изнутри ВМ — плагин предполагается запускать на той же машине, чью метадату он наблюдает.
* AWS IMDS не реализует GCE-эндпоинт; для AWS нужен отдельный source-плагин (TBD).

## Разработка

```bash
git clone <repo>
cd metadata-eda
pip install aiohttp pyyaml pytest pytest-asyncio
pytest tests/ -v
```

Тесты поднимают реальный HTTP-сервер на `aiohttp`, эмулирующий IMDS с `wait_for_change`, и проверяют end-to-end: long-poll-флоу, фолбэк на poll при 400-ответе, корректность state-файла между рестартами, отказ от невалидного YAML, и т.д.

## Лицензия

MIT.
