# config.py
import yaml
import os
import logging
from dataclasses import dataclass
from typing import Dict, Any

@dataclass(frozen=True)
class Config:
    db: Dict[str, Any]
    cluster: Dict[str, Any]
    slack_webhook_url: str

def is_debug() -> bool:
    # Replace with your actual debug logic.
    return True

def load_config() -> Config:
    if is_debug():
        config_file = 'debug_configs.yaml'
    else:
        logging.error("Only debug profile for now, exiting")
        exit(1)

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
