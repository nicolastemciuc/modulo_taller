from __future__ import annotations
from pathlib import Path
import os
import yaml

REPO_ROOT = Path(__file__).resolve().parent

def load_config() -> dict:
    path = REPO_ROOT / "config.yml"

    if not path.exists():
        raise SystemExit(f"No se encontró el archivo de configuración en {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CFG = load_config()
