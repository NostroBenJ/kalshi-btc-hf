# Running pm_hf on a server

The code runs the same on a $5-20/month Linux VM as it does on this PC. What a
server buys is (a) it never sleeps or closes with the desktop app, and (b) it
can sit closer to the exchange than a home connection.

**You create the server account yourself** (DigitalOcean, Vultr, AWS Lightsail,
etc.). Pick Ubuntu 24.04, the smallest plan, and a **US region** — Kalshi is
the venue, and you must be trading from where you actually are.

## 1. Find the fastest US region before paying for a month

From any candidate server (or a trial), run:

    python3 deploy/latency_probe.py

It times kept-alive requests to Kalshi's API and connects to Coinbase. Compare
the medians across regions; home measured ~20 ms kept-alive on 2026-09-13.
Pick the lowest. Don't guess where Kalshi's matching engine lives — measure.

## 2. Set up

    git clone <your copy> pm_hf   # or scp the folder
    cd pm_hf && bash deploy/setup_ubuntu.sh

This installs Python deps into a venv and the two systemd services, which
restart on crash and on reboot.

## 3. Keys (optional, you do this — never paste a key into chat)

    mkdir -p ~/.kalshi && chmod 700 ~/.kalshi
    nano ~/.kalshi/demo_key.pem          # paste the DEMO private key here
    chmod 600 ~/.kalshi/demo_key.pem
    sudo systemctl edit pm-hf-bot        # add, under [Service]:
      Environment=KALSHI_DEMO_KEY_ID=<your demo key id>
      Environment=KALSHI_DEMO_KEY_PATH=/home/<you>/.kalshi/demo_key.pem

A production key (KALSHI_KEY_ID / KALSHI_KEY_PATH) is used only for the
read-only websocket book. The code has no production order path.

## 4. Watch it

The dashboard binds to 127.0.0.1 only — it is not on the internet. Tunnel it:

    ssh -L 8765:localhost:8765 you@your-server

then open http://localhost:8765 on your own machine.

    journalctl -u pm-hf-bot -f      # bot log
    journalctl -u pm-hf-recorder -f # recorder log
