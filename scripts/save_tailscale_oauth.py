#!/usr/bin/env python3
"""Save operator credentials from masked terminal input, outside Git."""

import getpass
import os
from pathlib import Path

directory = Path(__file__).resolve().parents[1] / ".local/tailscale/credentials"
directory.mkdir(mode=0o700, parents=True, exist_ok=True)
directory.chmod(0o700)
if any((directory / name).exists() for name in ("client_id", "client_secret")):
    raise SystemExit("Credential files already exist; refusing to overwrite them.")
client_id = getpass.getpass("Tailscale operator client ID (hidden): ").strip()
secret = getpass.getpass("Tailscale operator client secret (hidden): ").strip()
if not client_id or not secret.startswith("tskey-client-"):
    raise SystemExit("Unexpected credential format; nothing saved.")
for name, value in (("client_id", client_id), ("client_secret", secret)):
    descriptor = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(value)
print("Operator credentials saved locally with owner-only permissions.")
