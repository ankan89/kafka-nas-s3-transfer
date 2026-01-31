"""
SingleStore client for tracking file transfer status.
Maintains a log of all processed messages and their status.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
import pymysql
from pymysql.cursors import DictCursor

from src.config_loader import ConfigLoader
from src.logging_config import CdpLogger
from scm_utilities.Constant.technical import Status


class TransferStatus:
    """Transfer status constants."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class SingleStoreClient:
    """
    Client for tracking file transfer status in SingleStore database.
    """

    # Table name for transfer logs
    TABLE_NAME = "file_transfer_log"

    def __init__(self):
        self._config = ConfigLoader()
        self._logger = CdpLogger.get_instance()

        # Get SingleStore configuration (Vault placeholders already resolved)
        self._db_config = self._config.get("singleStore", {})
        self._connection: Optional[pymysql.Connection] = None

    def connect(self) -> bool:
        """
        Connect to SingleStore database.

        Returns:
            True if connection successful
        """
        try:
            hostname = self._db_config.get("hostname", "")
            username = self._db_config.get("username", "")
            password = self._db_config.get("password", "")
            database = self._db_config.get("database", "ODM_FILES")

            if not all([hostname, username, password]):
                self._logger.log_error(
                    message="Missing required SingleStore configuration",
                    status=Status.Failed,
                    source="SingleStore"
                )
                return False

            self._connection = pymysql.connect(
                host=hostname,
                user=username,
                password=password,
                database=database,
                port=3306,
                cursorclass=DictCursor,
                autocommit=True,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30
            )

            self._logger.log_info(
                message=f"Connected to SingleStore: {hostname}/{database}",
                status=Status.Running,
                source="SingleStore"
            )

            # Ensure table exists
            self._ensure_table_exists()

            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to connect to SingleStore: {e}",
                status=Status.Failed,
                source="SingleStore"
            )
            return False

    def _ensure_connection(self) -> bool:
        """Ensure database connection is active."""
        if self._connection is None:
            return self.connect()

        try:
            self._connection.ping(reconnect=True)
            return True
        except Exception:
            return self.connect()

    def _ensure_table_exists(self):
        """Create transfer log table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            message_id VARCHAR(255) NOT NULL,
            event_type VARCHAR(50),
            st_bom_type VARCHAR(50),
            is_costed VARCHAR(20),
            olp_phase VARCHAR(50),
            source_path VARCHAR(1024),
            s3_path VARCHAR(1024),
            file_name VARCHAR(512),
            status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_message_id (message_id),
            KEY idx_status (status),
            KEY idx_created_at (created_at)
        )
        """

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(create_table_sql)
            self._logger.log_info(
                message=f"Ensured table {self.TABLE_NAME} exists",
                status=Status.Running,
                source="SingleStore"
            )
        except Exception as e:
            self._logger.log_warning(
                message=f"Could not create table {self.TABLE_NAME}: {e}",
                status=Status.Running,
                source="SingleStore"
            )

    def insert_pending(self, message: Dict[str, Any]) -> bool:
        """
        Insert a new pending transfer record.

        Args:
            message: Kafka message with file metadata

        Returns:
            True if insert successful
        """
        if not self._ensure_connection():
            return False

        message_id = message.get("MESSAGE_ID", "")

        try:
            insert_sql = f"""
            INSERT INTO {self.TABLE_NAME}
            (message_id, event_type, st_bom_type, is_costed, olp_phase,
             source_path, file_name, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            updated_at = CURRENT_TIMESTAMP
            """

            source_path = f"{message.get('ST_BOM_DOC_PATH', '')}/{message.get('ST_BOM_FILE_NAME', '')}"

            with self._connection.cursor() as cursor:
                cursor.execute(insert_sql, (
                    message_id,
                    message.get("EVENT_TYPE", ""),
                    message.get("ST_BOM_TYPE", ""),
                    message.get("IS_COSTED", ""),
                    message.get("OLP_PHASE", ""),
                    source_path,
                    message.get("ST_BOM_FILE_NAME", ""),
                    TransferStatus.PENDING
                ))

            self._logger.log_info(
                message=f"Inserted PENDING record for message: {message_id}",
                status=Status.Running,
                correlation_id=message_id,
                source="SingleStore"
            )
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to insert pending record: {e}",
                status=Status.Failed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return False

    def update_in_progress(self, message_id: str) -> bool:
        """Update record status to IN_PROGRESS."""
        return self._update_status(message_id, TransferStatus.IN_PROGRESS)

    def update_success(self, message_id: str, s3_path: str) -> bool:
        """
        Update record status to SUCCESS with S3 path.

        Args:
            message_id: The MESSAGE_ID
            s3_path: The S3 path where file was uploaded

        Returns:
            True if update successful
        """
        if not self._ensure_connection():
            return False

        try:
            update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET status = %s, s3_path = %s, error_message = NULL
            WHERE message_id = %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(update_sql, (
                    TransferStatus.SUCCESS,
                    s3_path,
                    message_id
                ))

            self._logger.log_info(
                message=f"Updated record to SUCCESS for message: {message_id}",
                status=Status.Completed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to update success status: {e}",
                status=Status.Failed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return False

    def update_failed(self, message_id: str, error_message: str) -> bool:
        """
        Update record status to FAILED with error message.

        Args:
            message_id: The MESSAGE_ID
            error_message: Error description

        Returns:
            True if update successful
        """
        if not self._ensure_connection():
            return False

        try:
            update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET status = %s, error_message = %s
            WHERE message_id = %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(update_sql, (
                    TransferStatus.FAILED,
                    error_message[:4000] if error_message else None,  # Truncate long errors
                    message_id
                ))

            self._logger.log_info(
                message=f"Updated record to FAILED for message: {message_id}",
                status=Status.Failed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to update failed status: {e}",
                status=Status.Failed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return False

    def update_skipped(self, message_id: str, reason: str) -> bool:
        """Update record status to SKIPPED."""
        if not self._ensure_connection():
            return False

        try:
            update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET status = %s, error_message = %s
            WHERE message_id = %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(update_sql, (
                    TransferStatus.SKIPPED,
                    reason,
                    message_id
                ))

            return True

        except Exception:
            return False

    def _update_status(self, message_id: str, status: str) -> bool:
        """Generic status update."""
        if not self._ensure_connection():
            return False

        try:
            update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET status = %s
            WHERE message_id = %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(update_sql, (status, message_id))

            return True

        except Exception:
            return False

    def get_record(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get transfer record by message ID.

        Args:
            message_id: The MESSAGE_ID to look up

        Returns:
            Record dict or None if not found
        """
        if not self._ensure_connection():
            return None

        try:
            select_sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE message_id = %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(select_sql, (message_id,))
                return cursor.fetchone()

        except Exception:
            return None

    def get_failed_records(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent failed transfer records."""
        if not self._ensure_connection():
            return []

        try:
            select_sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(select_sql, (TransferStatus.FAILED, limit))
                return cursor.fetchall()

        except Exception:
            return []

    def close(self):
        """Close database connection."""
        try:
            if self._connection:
                self._connection.close()
                self._connection = None

            self._logger.log_info(
                message="SingleStore connection closed",
                status=Status.Completed,
                source="SingleStore"
            )

        except Exception as e:
            self._logger.log_error(
                message=f"Error closing SingleStore connection: {e}",
                status=Status.Failed,
                source="SingleStore"
            )
