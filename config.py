"""Config loader utility for BalancerWithVSCode.

Provides `load_or_create_config(path, defaults)` which loads JSON config
or creates it with the provided defaults if missing.
"""

import json
import os
from typing import Any, Dict


def _atomic_write(path: str, data: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def load_or_create_config(path: str, defaults: Dict[str, Any]) -> Dict[str, Any]:
    """Load JSON config from `path` or create it with `defaults`.

    Returns a dict with config values. If the file exists but is missing keys,
    missing keys are added and the file is updated.
    """
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # ensure defaults are present
            changed = False
            for k, v in defaults.items():
                if k not in data:
                    data[k] = v
                    changed = True
            if changed:
                _atomic_write(path, data)
            return data
        except Exception:
            # corrupted file: back it up and recreate defaults
            try:
                os.replace(path, path + ".backup")
            except Exception:
                pass
            _atomic_write(path, defaults)
            return dict(defaults)
    else:
        # create parent dir if necessary
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        _atomic_write(path, defaults)
        return dict(defaults)
