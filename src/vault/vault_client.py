"""
HashiCorp Vault client for retrieving secrets.
Supports Kubernetes authentication for KOB deployment.
"""

import os
import re
import hvac
from typing import Optional, Dict, Any
from functools import lru_cache


class VaultClient:
    """Client for interacting with HashiCorp Vault."""

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

        self._vault_addr = os.getenv("VAULT_ADDR", "https://vault.dell.com/")
        self._vault_token = os.getenv("VAULT_TOKEN")
        self._vault_role = os.getenv("VAULT_ROLE", "vault-auth")
        self._k8s_token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"

        self._client: Optional[hvac.Client] = None
        self._secrets_cache: Dict[str, Any] = {}
        self._initialized = True

    def _get_k8s_token(self) -> Optional[str]:
        """Read Kubernetes service account token."""
        try:
            if os.path.exists(self._k8s_token_path):
                with open(self._k8s_token_path, 'r') as f:
                    return f.read().strip()
        except Exception:
            pass
        return None

    def connect(self) -> bool:
        """
        Connect to Vault using available authentication method.
        Priority: Token auth > Kubernetes auth
        """
        try:
            self._client = hvac.Client(url=self._vault_addr)

            # Try token auth first (for local development)
            if self._vault_token:
                self._client.token = self._vault_token
                if self._client.is_authenticated():
                    return True

            # Try Kubernetes auth (for KOB deployment)
            k8s_token = self._get_k8s_token()
            if k8s_token:
                self._client.auth.kubernetes.login(
                    role=self._vault_role,
                    jwt=k8s_token
                )
                if self._client.is_authenticated():
                    return True

            return False

        except Exception as e:
            print(f"Failed to connect to Vault: {e}")
            return False

    @lru_cache(maxsize=100)
    def get_secret(self, path: str) -> Optional[str]:
        """
        Get a secret from Vault KV engine.

        Args:
            path: Secret path (e.g., "kv/data/prod/commons/kafka_username")

        Returns:
            Secret value or None if not found
        """
        if not self._client or not self._client.is_authenticated():
            if not self.connect():
                raise ConnectionError("Unable to authenticate with Vault")

        try:
            # Parse path to extract mount point and secret path
            # Expected format: kv/data/prod/commons/kafka_username
            parts = path.split('/')
            if len(parts) < 4:
                return None

            mount_point = parts[0]  # "kv"
            # Skip "data" in path for v2 KV engine
            secret_path = '/'.join(parts[2:-1])  # "prod/commons"
            key = parts[-1]  # "kafka_username"

            response = self._client.secrets.kv.v2.read_secret_version(
                path=secret_path,
                mount_point=mount_point
            )

            if response and 'data' in response and 'data' in response['data']:
                return response['data']['data'].get(key)

            return None

        except Exception as e:
            print(f"Failed to get secret from path {path}: {e}")
            return None

    def resolve_placeholders(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively resolve Vault placeholders in configuration.

        Placeholders format: {{kv/data/path/to/secret}}

        Args:
            config: Configuration dictionary with potential placeholders

        Returns:
            Configuration with resolved secrets
        """
        if isinstance(config, dict):
            return {k: self.resolve_placeholders(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self.resolve_placeholders(item) for item in config]
        elif isinstance(config, str):
            match = self.VAULT_PLACEHOLDER_PATTERN.match(config)
            if match:
                secret_path = match.group(1)
                secret_value = self.get_secret(secret_path)
                if secret_value:
                    return secret_value
                else:
                    print(f"Warning: Could not resolve secret at {secret_path}")
            return config
        else:
            return config

    def close(self):
        """Close Vault client connection."""
        if self._client:
            self._client.adapter.close()
            self._client = None


# Module-level functions for convenience
def get_vault_client() -> VaultClient:
    """Get singleton Vault client instance."""
    return VaultClient()


def resolve_config_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve Vault placeholders in configuration."""
    client = get_vault_client()
    return client.resolve_placeholders(config)
