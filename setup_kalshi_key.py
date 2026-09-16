"""A small window to install a Kalshi READ-ONLY API key on this computer.

    python setup_kalshi_key.py

You paste the Key ID and the private key into the window. It then:
  1. saves the private key to C:\\Users\\<you>\\.kalshi\\kalshi_read.pem
  2. sets KALSHI_KEY_ID and KALSHI_KEY_PATH for your Windows account
  3. signs one read-only request to Kalshi and shows whether the key works

Nothing is printed, logged, or sent anywhere except that one signed request to
Kalshi. The key never leaves this machine otherwise.
"""

import os
import subprocess
import sys
import tkinter as tk
from pathlib import Path
from tkinter import messagebox

HERE = Path(__file__).parent
KEY_DIR = Path.home() / ".kalshi"
KEY_FILE = KEY_DIR / "kalshi_read.pem"


def save():
    key_id = id_box.get().strip()
    pem = key_box.get("1.0", "end").strip()
    if not key_id or "BEGIN" in key_id:
        messagebox.showerror("Key ID", "The top box needs the short Key ID, not the long private key.")
        return
    if "BEGIN" not in pem or "PRIVATE KEY" not in pem:
        messagebox.showerror("Private key", "The big box needs the whole private key, including the\n"
                                            "-----BEGIN ... PRIVATE KEY----- and -----END ... PRIVATE KEY----- lines.")
        return
    try:
        from cryptography.hazmat.primitives import serialization
        serialization.load_pem_private_key(pem.encode(), password=None)
    except Exception:
        messagebox.showerror("Private key", "That text is not a valid private key. Copy it again from Kalshi,\n"
                                            "all of it, and paste it into the big box.")
        return

    KEY_DIR.mkdir(exist_ok=True)
    KEY_FILE.write_text(pem + "\n", encoding="ascii")
    # set for future programs (Windows account) and for this process (for the check below)
    subprocess.run(["setx", "KALSHI_KEY_ID", key_id], capture_output=True, check=True)
    subprocess.run(["setx", "KALSHI_KEY_PATH", str(KEY_FILE)], capture_output=True, check=True)
    os.environ["KALSHI_KEY_ID"], os.environ["KALSHI_KEY_PATH"] = key_id, str(KEY_FILE)
    key_box.delete("1.0", "end")

    status.config(text="Saved. Checking with Kalshi ...", fg="#e6b450")
    root.update()
    sys.path.insert(0, str(HERE))
    import kalshi_client as kc
    try:
        _, rtt = kc.Rest("prod", kc.load_key("prod")).request("GET", "/portfolio/balance", signed=True)
        status.config(text=f"Works. Kalshi accepted the key ({rtt:.0f} ms).\n"
                           "Now fully quit and reopen the Claude app, then tell Claude it's done.", fg="#26a69a")
    except Exception as e:
        msg = str(e)
        hint = ("Kalshi rejected it: check the Key ID matches this private key\n"
                "(each new key has its own ID)." if "401" in msg or "403" in msg else msg[:200])
        status.config(text=f"Saved, but the check failed.\n{hint}", fg="#ef5350")


root = tk.Tk()
root.title("Kalshi read-only key setup")
root.configure(bg="#0f141c", padx=16, pady=14)
label = dict(bg="#0f141c", fg="#cdd6e4", anchor="w", justify="left", font=("Segoe UI", 10))

tk.Label(root, text="1.  Key ID  (short code, like a1b2c3d4-...)", **label).pack(fill="x")
id_box = tk.Entry(root, width=70, font=("Consolas", 10))
id_box.pack(fill="x", pady=(2, 12))

tk.Label(root, text="2.  Private key  (the long block, BEGIN ... END lines included)", **label).pack(fill="x")
key_box = tk.Text(root, width=70, height=14, font=("Consolas", 9))
key_box.pack(fill="both", expand=True, pady=(2, 12))

tk.Button(root, text="Save and check", command=save, font=("Segoe UI", 10, "bold"), padx=12, pady=4).pack(anchor="w")
status = tk.Label(root, text="Use a key with ONLY 'Read all data' checked.", **label)
status.pack(fill="x", pady=(10, 0))

root.mainloop()
