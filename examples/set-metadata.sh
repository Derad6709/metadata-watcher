#!/usr/bin/env bash
# Примеры заливки плейбука в метадату ВМ — то, что увидит плагин.
# Запусти ОДИН из блоков ниже, не оба.

set -euo pipefail

# --------------------------------------------------------------------------- #
# 1. Yandex.Cloud
# --------------------------------------------------------------------------- #
cat > /tmp/play.yml <<'EOF'
- hosts: localhost
  connection: local
  gather_facts: false
  tasks:
    - name: triggered from metadata
      ansible.builtin.debug:
        msg: "Hello from metadata-driven playbook, etag={{ metadata_change.etag }}"
EOF

cat > /tmp/extra.yml <<'EOF'
greeting: "Привет"
target_env: production
EOF

VM_NAME="my-vm"

yc compute instance add-metadata "$VM_NAME" \
    --metadata-from-file ansible-playbook=/tmp/play.yml \
    --metadata-from-file ansible-extra-vars=/tmp/extra.yml

# --------------------------------------------------------------------------- #
# 2. Google Compute Engine
# --------------------------------------------------------------------------- #
# gcloud compute instances add-metadata my-vm \
#     --zone=us-central1-a \
#     --metadata-from-file ansible-playbook=/tmp/play.yml,ansible-extra-vars=/tmp/extra.yml

# --------------------------------------------------------------------------- #
# 3. Variant: только URL в метадате (kind: ref)
# --------------------------------------------------------------------------- #
# yc compute instance add-metadata "$VM_NAME" \
#     --metadata ansible-playbook-url=https://git.example.com/playbooks/postgres-tuning.yml
