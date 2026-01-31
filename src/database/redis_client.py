"""
Redis client for message deduplication.
Tracks processed MESSAGE_IDs to prevent duplicate processing.
"""

from typing import Optional
import redis
from redis.exceptions import RedisError

from src.config_loader import ConfigLoader
from src.logging_config import CdpLogger
from scm_utilities.Constant.technical import Status


class RedisClient:
    """
    Redis client for deduplication of processed messages.
    Supports primary/secondary Redis for failover.
    """

    # Default TTL for processed message keys (7 days)
    DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60

    def __init__(self):
        self._config = ConfigLoader()
        self._logger = CdpLogger.get_instance()

        self._primary_client: Optional[redis.Redis] = None
        self._secondary_client: Optional[redis.Redis] = None

        # Get Redis configuration
        self._primary_config = self._config.get("primaryRedis", {})
        self._secondary_config = self._config.get("secondaryRedis", {})

        # Key prefix for processed messages
        self._key_prefix = "processed:"

    def connect(self) -> bool:
        """
        Connect to Redis (primary and secondary).

        Returns:
            True if at least one connection successful
        """
        primary_ok = self._connect_primary()
        secondary_ok = self._connect_secondary()

        return primary_ok or secondary_ok

    def _connect_primary(self) -> bool:
        """Connect to primary Redis."""
        try:
            hostname = self._primary_config.get("hostname", "")
            port = self._primary_config.get("port", 6379)
            password = self._primary_config.get("password", "")

            if not hostname:
                self._logger.log_warning(
                    message="Primary Redis hostname not configured",
                    status=Status.Running,
                    source="Redis"
                )
                return False

            self._primary_client = redis.Redis(
                host=hostname,
                port=port,
                password=password if password and password != "***" else None,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )

            # Test connection
            self._primary_client.ping()

            self._logger.log_info(
                message=f"Connected to primary Redis: {hostname}:{port}",
                status=Status.Running,
                source="Redis"
            )
            return True

        except RedisError as e:
            self._logger.log_warning(
                message=f"Failed to connect to primary Redis: {e}",
                status=Status.Running,
                source="Redis"
            )
            return False

    def _connect_secondary(self) -> bool:
        """Connect to secondary Redis."""
        try:
            hostname = self._secondary_config.get("hostname", "")
            port = self._secondary_config.get("port", 6379)
            password = self._secondary_config.get("password", "")

            if not hostname:
                return False

            self._secondary_client = redis.Redis(
                host=hostname,
                port=port,
                password=password if password and password != "***" else None,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )

            # Test connection
            self._secondary_client.ping()

            self._logger.log_info(
                message=f"Connected to secondary Redis: {hostname}:{port}",
                status=Status.Running,
                source="Redis"
            )
            return True

        except RedisError as e:
            self._logger.log_warning(
                message=f"Failed to connect to secondary Redis: {e}",
                status=Status.Running,
                source="Redis"
            )
            return False

    def _get_active_client(self) -> Optional[redis.Redis]:
        """Get an active Redis client (primary preferred)."""
        if self._primary_client:
            try:
                self._primary_client.ping()
                return self._primary_client
            except RedisError:
                pass

        if self._secondary_client:
            try:
                self._secondary_client.ping()
                return self._secondary_client
            except RedisError:
                pass

        return None

    def is_processed(self, message_id: str) -> bool:
        """
        Check if a message has already been processed.

        Args:
            message_id: The MESSAGE_ID to check

        Returns:
            True if already processed, False otherwise
        """
        client = self._get_active_client()
        if not client:
            # If Redis is unavailable, assume not processed
            self._logger.log_warning(
                message=f"Redis unavailable, assuming message {message_id} not processed",
                status=Status.Running,
                correlation_id=message_id,
                source="Redis"
            )
            return False

        try:
            key = f"{self._key_prefix}{message_id}"
            exists = client.exists(key)
            return bool(exists)

        except RedisError as e:
            self._logger.log_warning(
                message=f"Redis error checking message {message_id}: {e}",
                status=Status.Running,
                correlation_id=message_id,
                source="Redis"
            )
            return False

    def mark_processed(self, message_id: str, ttl_seconds: int = None) -> bool:
        """
        Mark a message as processed.

        Args:
            message_id: The MESSAGE_ID to mark
            ttl_seconds: Optional TTL for the key (default: 7 days)

        Returns:
            True if successfully marked
        """
        client = self._get_active_client()
        if not client:
            self._logger.log_warning(
                message=f"Redis unavailable, cannot mark message {message_id} as processed",
                status=Status.Running,
                correlation_id=message_id,
                source="Redis"
            )
            return False

        try:
            key = f"{self._key_prefix}{message_id}"
            ttl = ttl_seconds or self.DEFAULT_TTL_SECONDS

            # Set key with TTL
            client.setex(key, ttl, "1")

            self._logger.log_info(
                message=f"Marked message {message_id} as processed (TTL: {ttl}s)",
                status=Status.Completed,
                correlation_id=message_id,
                source="Redis"
            )
            return True

        except RedisError as e:
            self._logger.log_warning(
                message=f"Redis error marking message {message_id}: {e}",
                status=Status.Running,
                correlation_id=message_id,
                source="Redis"
            )
            return False

    def remove_processed(self, message_id: str) -> bool:
        """
        Remove a message from processed set (for reprocessing).

        Args:
            message_id: The MESSAGE_ID to remove

        Returns:
            True if successfully removed
        """
        client = self._get_active_client()
        if not client:
            return False

        try:
            key = f"{self._key_prefix}{message_id}"
            client.delete(key)
            return True

        except RedisError:
            return False

    def get_processed_count(self) -> int:
        """
        Get count of processed messages in Redis.

        Returns:
            Count of processed message keys
        """
        client = self._get_active_client()
        if not client:
            return -1

        try:
            pattern = f"{self._key_prefix}*"
            count = 0
            for _ in client.scan_iter(pattern, count=1000):
                count += 1
            return count

        except RedisError:
            return -1

    def close(self):
        """Close Redis connections."""
        try:
            if self._primary_client:
                self._primary_client.close()
                self._primary_client = None

            if self._secondary_client:
                self._secondary_client.close()
                self._secondary_client = None

            self._logger.log_info(
                message="Redis connections closed",
                status=Status.Completed,
                source="Redis"
            )

        except Exception as e:
            self._logger.log_error(
                message=f"Error closing Redis connections: {e}",
                status=Status.Failed,
                source="Redis"
            )
