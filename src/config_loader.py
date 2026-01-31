"""
Configuration loader with Vault secret resolution.
Loads config.json and resolves {{kv/data/...}} placeholders.
"""

import json
import os
from typing import Any, Optional
from src.vault.vault_client import resolve_config_secrets


class ConfigLoader:
    """
    Singleton configuration loader.
    Loads config.json and resolves Vault placeholders.
    """

    _instance = None
    _config: Optional[dict] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self._load_config()

    def _load_config(self):
        """Load and resolve configuration."""
        config_path = os.getenv("CONFIG_PATH", "./config/config.json")

        try:
            with open(config_path, 'r') as f:
                raw_config = json.load(f)

            # Resolve Vault placeholders
            self._config = resolve_config_secrets(raw_config)

        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key.

        Args:
            key: Top-level configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        if self._config is None:
            return default
        return self._config.get(key, default)

    def get_nested(self, *keys: str, default: Any = None) -> Any:
        """
        Get nested configuration value.

        Args:
            *keys: Path of keys to traverse
            default: Default value if path not found

        Returns:
            Configuration value or default
        """
        value = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value

    def reload(self):
        """Reload configuration from file."""
        self._config = None
        self._load_config()

    @property
    def raw_config(self) -> dict:
        """Get the full resolved configuration."""
        return self._config or {}
