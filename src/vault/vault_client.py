import json
import os

# from src.load_config_properties import get_vault_creds
from src.util.load_secrets import add_secrets_from_vault


def get_vault_creds():
    role_id_path = "/vault-secrets/roleid"
    secret_id_path = "/vault-secrets/secretid"
    ns_path = "/vault-secrets/vaultns"

    try:
        with open(role_id_path, "r") as role_id_file, open(secret_id_path, "r") as secret_id_file, open(ns_path,
                                                                                                        "r") as ns_file:
            role_id = role_id_file.read().strip()
            secret_id = secret_id_file.read().strip()
            ns = ns_file.read().strip()

        return role_id, secret_id, ns
    except FileNotFoundError:
        # logger.log(logging.ERROR, f"Failed to read Vault config. File not found at {role_id_path} or {secret_id_path} or {ns_path}")
        return None, None, None

def load_config(configProp):
    environment = os.getenv('ENVIRONMENT', 'local').lower()
    if environment == 'local':
        role_id = configProp.get("vault", {}).get("role_id")
        secret_id = configProp.get("vault", {}).get("secret_id")
        ns = configProp.get("vault", {}).get("ns")
    elif environment == 'kob':
        role_id, secret_id, ns = get_vault_creds()
    else:
        raise ValueError(f"Unknown environment: {environment}")

    vault_addr = configProp.get("vault", {}).get("vault_addr", "https://hcvault-nonprod.dell.com")

    if not (role_id and secret_id and ns and vault_addr):
        raise Exception(
            f"Missing vault_addr:{vault_addr}, role_id:{role_id}, secret_id:{secret_id}, or ns:{ns} in config")
    updated_config = add_secrets_from_vault(configProp, vault_addr, role_id, secret_id, ns)
    # TODO: added for testing need to remove
    #print(json.dumps(updated_config))
    return updated_config


class ConfigLoader:
    _config = None
    global config_file_path
    _instance = None
    environment = os.getenv('ENVIRONMENT', 'local').lower()
    if environment == 'local':
        config_file_path = "./src/config/config.json"
    elif environment == 'kob':
        config_file_path = "/scm-config/config.json"

    def __new__(cls, config_path=config_file_path):

        if cls._instance is None:
            #print(f'config path is, {config_path}')
            cls._instance = super().__new__(cls)
            with open(config_path, "r") as file:
                cls._config = json.load(file)
            cls._config = load_config(cls._config)

        return cls._instance

    def get(self, key, default=None):
        return self._config.get(key, default)

    def to_json(self):
        """Convert config_data to JSON string."""
        return json.dumps(self._config)
