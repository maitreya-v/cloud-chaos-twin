"""Read-only availability check; does not start services or read credentials."""

import os
import platform
import shutil
import sys


def environment_report(root):
    tools = {name: shutil.which(name) is not None for name in ("git", "docker", "kubectl", "kind", "helm")}
    return {
        "python": platform.python_version(),
        "offline_demo_ready": sys.version_info >= (3, 9),
        "os": platform.system(),
        "architecture": platform.machine(),
        "reported_cpu_count": os.cpu_count(),
        "free_disk_gib": round(shutil.disk_usage(root).free / (1024 ** 3), 1),
        "commands_available": tools,
        "missing_live_commands": [name for name, present in tools.items() if not present],
        "notes": [
            "Command presence does not verify a running Docker daemon or a working cluster.",
            "RAM and container memory limits must be recorded separately.",
            "No services were started and no software was installed.",
        ],
    }
