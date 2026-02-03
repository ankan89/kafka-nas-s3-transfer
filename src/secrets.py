"""
NAS credentials for SMB/CIFS access.
These credentials are stored locally and NOT fetched from Vault.
In production, mount these as Kubernetes secrets via environment variables.
"""

import os


class NASCredentials:
    """NAS credentials configuration for SMB/CIFS access."""

    def __init__(self):
        self._username = "svc_npplmdp"
        self._password = "Q0H9V4a31i*_5jCK+ouMnzXB"
        self._domain = "AMERICAS"

    @property
    def username(self) -> str:
        """Get NAS username."""
        return self._username

    @property
    def password(self) -> str:
        """Get NAS password."""
        return self._password

    @property
    def domain(self) -> str:
        """Get NAS domain."""
        return self._domain

    def get_credentials(self) -> dict:
        """Return credentials as dictionary."""
        return {
            "username": self._username,
            "password": self._password,
            "domain": self._domain
        }

    def validate(self) -> bool:
        """Validate that required credentials are set."""
        if not self._username or not self._password:
            return False
        return True


# Singleton instance
_nas_credentials = None


def get_nas_credentials() -> NASCredentials:
    """Get singleton NAS credentials instance."""
    global _nas_credentials
    if _nas_credentials is None:
        _nas_credentials = NASCredentials()
    return _nas_credentials
