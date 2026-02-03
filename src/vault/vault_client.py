"""
HashiCorp Vault client for retrieving secrets.
Supports AppRole authentication for local and KOB deployment.
"""

import os
import re
import hvac
from typing import Optional, Dict, Any
from functools import lru_cache


def get_vault_creds():
    """
    Get Vault credentials from Kubernetes environment.
    Returns role_id, secret_id, and namespace.
    """
    role_id = os.getenv("VAULT_ROLE_ID")
    secret_id = os.getenv("VAULT_SECRET_ID")
    ns = os.getenv("VAULT_NAMESPACE")
    return role_id, secret_id, ns


def add_secrets_from_vault(config: Dict[str, Any], vault_addr: str,
                           role_id: str, secret_id: str, ns: str) -> Dict[str, Any]:
    """
    Resolve Vault placeholders in configuration using AppRole auth.

    Args:
        config: Configuration dictionary with potential placeholders
        vault_addr: Vault server address
        role_id: AppRole role ID
        secret_id: AppRole secret ID
        ns: Vault namespace

    Returns:
        Configuration with resolved secrets
    """
    client = hvac.Client(url=vault_addr, namespace=ns, verify="./src/root.crt")

    # Authenticate using AppRole
    client.auth.approle.login(role_id=role_id, secret_id=secret_id)

    if not client.is_authenticated():
        raise Exception("Failed to authenticate with Vault")

    # Resolve all placeholders
    resolved_config = _resolve_placeholders(config, client)

    return resolved_config


def _resolve_placeholders(config: Any, client: hvac.Client) -> Any:
    """
    Recursively resolve Vault placeholders in configuration.

    Placeholders format: {{kv/data/path/to/secret}}
    """
    placeholder_pattern = re.compile(r'\{\{(kv/data/[^}]+)\}\}')

    if isinstance(config, dict):
        return {k: _resolve_placeholders(v, client) for k, v in config.items()}
    elif isinstance(config, list):
        return [_resolve_placeholders(item, client) for item in config]
    elif isinstance(config, str):
        match = placeholder_pattern.match(config)
        if match:
            secret_path = match.group(1)
            secret_value = _get_secret(client, secret_path)
            if secret_value:
                return secret_value
            else:
                print(f"Warning: Could not resolve secret at {secret_path}")
        return config
    else:
        return config


def _get_secret(client: hvac.Client, path: str) -> Optional[str]:
    """
    Get a secret from Vault KV engine.

    Args:
        client: Authenticated Vault client
        path: Secret path (e.g., "kv/data/prod/commons/kafka_username")

    Returns:
        Secret value or None if not found
    """
    try:
        # Parse path to extract mount point and secret path
        # Expected format: kv/data/prod/commons/kafka_username
        parts = path.split('/')
        if len(parts) < 4:
            return None

        mount_point = f"secret/{parts[0]}"  # "kv"
        # Skip "data" in path for v2 KV engine
        secret_path = '/'.join(parts[2:-1])  # "prod/commons"
        key = parts[-1]  # "kafka_username"

        response = client.secrets.kv.v2.read_secret_version(
            path=secret_path,
            mount_point=mount_point
        )

        if response and 'data' in response and 'data' in response['data']:
            return response['data']['data'].get(key)

        return None

    except Exception as e:
        print(f"Failed to get secret from path {path}: {e}")
        return None


def load_config(configProp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load configuration and resolve Vault secrets based on environment.

    For 'local' environment: uses role_id, secret_id, ns from config
    For 'kob' environment: uses environment variables

    Args:
        configProp: Raw configuration dictionary

    Returns:
        Configuration with resolved secrets
    """
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

    return updated_config


# Legacy functions for backward compatibility
class VaultClient:
    """Legacy client - use load_config() instead."""

    _instance = None
    VAULT_PLACEHOLDER_PATTERN = re.compile(r'\{\{(kv/data/[^}]+)\}\}')

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

    def resolve_placeholders(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve using the new load_config function."""
        return load_config(config)


def get_vault_client() -> VaultClient:
    """Get singleton Vault client instance."""
    return VaultClient()


def resolve_config_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve Vault placeholders in configuration."""
    return load_config(config)
