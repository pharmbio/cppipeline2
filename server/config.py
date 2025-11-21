# config.py
import yaml
import os
import logging
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import Dict, Any

# Load environment variables early so CONFIG sees values from .env in dev
load_dotenv()

@dataclass(frozen=True)
class Config:
    db: Dict[str, Any]
    cluster: Dict[str, Any]
    slack_webhook_url: str

def is_debug() -> bool:
    """
    Determine debug mode from the DEBUG env var.

    Truthy: 1, true, yes, on
    Falsy: 0, false, no, off, empty/missing
    """
    val = os.environ.get('DEBUG')
    if val is None:
        return False
    v = str(val).strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return False
    return True

def load_config() -> Config:
    # Allow override of config path via env; default to debug_configs.yaml
    config_file = os.getenv('CPP_CONFIG_FILE', 'debug_configs.yaml')
    with open(config_file, 'r') as f:
        config_data = yaml.load(f, Loader=yaml.FullLoader)

    # Prefer environment variable for secrets like Slack webhook; fallback to file for dev
    slack_webhook = os.getenv('SLACK_WEBHOOK_URL', config_data.get('slack_webhook_url', ''))

    # Merge DB password from env into postgres config (do not store secrets in YAML)
    pg_cfg = dict(config_data.get('postgres', {}))
    pg_cfg['password'] = os.getenv('DB_PASS', pg_cfg.get('password', ''))

    return Config(
        db=pg_cfg,
        cluster=config_data['cluster'],
        slack_webhook_url=slack_webhook
    )

# Load the configuration once at module import.
CONFIG = load_config()

if __name__ == "__main__":
    # For testing purposes:
    print("Postgres DB:", CONFIG.db)
    print("Dardel User:", CONFIG.cluster.get("dardel", {}).get("user"))
