#!/usr/bin/env bash
# Install pm_hf on Ubuntu 24.04 as two systemd services. Run from the pm_hf folder.
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
USER_NAME="$(whoami)"

sudo apt-get update -y
sudo apt-get install -y python3 python3-venv chrony   # chrony: keep the clock right (this PC ran 3.2 s slow)

python3 -m venv "$HERE/.venv"
"$HERE/.venv/bin/pip" install --upgrade pip
"$HERE/.venv/bin/pip" install websockets cryptography

BROKER="${BROKER:-paper}"   # BROKER=demo bash deploy/setup_ubuntu.sh once a demo key is configured

# Read-only production key for the websocket book. The ID is not secret; the .pem is,
# and must already be at ~/.kalshi/kalshi_read.pem (chmod 600). Usage:
#   KALSHI_KEY_ID=<your key id> bash deploy/setup_ubuntu.sh
KEY_ENV=""
if [ -n "${KALSHI_KEY_ID:-}" ] && [ -f "$HOME/.kalshi/kalshi_read.pem" ]; then
  chmod 700 "$HOME/.kalshi" && chmod 600 "$HOME/.kalshi/kalshi_read.pem"
  KEY_ENV="Environment=KALSHI_KEY_ID=${KALSHI_KEY_ID}
Environment=KALSHI_KEY_PATH=$HOME/.kalshi/kalshi_read.pem"
  echo "Kalshi read-only key found: websocket book enabled"
else
  echo "No Kalshi key configured: the bot will poll the book over REST"
fi

sudo tee /etc/systemd/system/pm-hf-bot.service >/dev/null <<UNIT
[Unit]
Description=pm_hf Kalshi bot (${BROKER}) + dashboard on 127.0.0.1:8765
After=network-online.target
Wants=network-online.target

[Service]
User=${USER_NAME}
WorkingDirectory=${HERE}
${KEY_ENV}
ExecStart=${HERE}/.venv/bin/python paper_bot.py --broker ${BROKER}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

sudo tee /etc/systemd/system/pm-hf-recorder.service >/dev/null <<UNIT
[Unit]
Description=pm_hf Kalshi + US spot recorder (the Polymarket recorder is ~13 GB/day and stays off)
After=network-online.target
Wants=network-online.target

[Service]
User=${USER_NAME}
WorkingDirectory=${HERE}
${KEY_ENV}
ExecStart=${HERE}/.venv/bin/python kalshi_recorder.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable pm-hf-bot pm-hf-recorder
# restart, not just start: `enable --now` leaves an already-running service on its OLD
# environment, which on 2026-09-13 kept the bot polling after the key was installed
sudo systemctl restart pm-hf-bot pm-hf-recorder
systemctl --no-pager status pm-hf-bot pm-hf-recorder | head -20
echo "Dashboard: ssh -L 8765:localhost:8765 ${USER_NAME}@<this-server>  then open http://localhost:8765"
