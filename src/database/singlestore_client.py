"""
SingleStore client for tracking file transfer status.
Maintains a log of all processed messages and their status in NAS_File_Tracker table.
"""

import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import pymysql
from pymysql.cursors import DictCursor

from src.config_loader import ConfigLoader
from src.logging_config import CdpLogger
from scm_utilities.Constant.technical import Status


class TransferStatus:
    """Transfer status constants."""
    CREATED = "Created"
    PROCESSED = "Processed"
    FAILED = "Failed"


class SingleStoreClient:
    """
    Client for tracking file transfer status in SingleStore database.
    Uses the existing NAS_File_Tracker table.
    """

    # Table name for transfer logs - using existing table
    TABLE_NAME = "NAS_File_Tracker"

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
        """Verify the NAS_File_Tracker table exists."""
        check_table_sql = f"""
        SELECT COUNT(*) as cnt FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """

        try:
            database = self._db_config.get("database", "ODM_FILES")
            with self._connection.cursor() as cursor:
                cursor.execute(check_table_sql, (database, self.TABLE_NAME))
                result = cursor.fetchone()
                if result and result.get('cnt', 0) > 0:
                    self._logger.log_info(
                        message=f"Verified table {self.TABLE_NAME} exists",
                        status=Status.Running,
                        source="SingleStore"
                    )
                else:
                    self._logger.log_warning(
                        message=f"Table {self.TABLE_NAME} does not exist in database {database}",
                        status=Status.Running,
                        source="SingleStore"
                    )
        except Exception as e:
            self._logger.log_warning(
                message=f"Could not verify table {self.TABLE_NAME}: {e}",
                status=Status.Running,
                source="SingleStore"
            )

    def insert_created(self, message: Dict[str, Any]) -> bool:
        """
        Insert a new record with status 'Created' when Kafka message is received.

        Args:
            message: Kafka message with file metadata (entire message stored in Kafka_Event)

        Returns:
            True if insert successful
        """
        if not self._ensure_connection():
            return False

        message_id = message.get("MESSAGE_ID", "")

        try:
            insert_sql = f"""
            INSERT INTO {self.TABLE_NAME}
            (MESSAGE_ID, Kafka_Event, EVENT_TYPE, ST_BOM_DOC_PATH, ST_BOM_FILE_NAME,
             EVENT_TIMESTAMP, Processed_Timestamp, Status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Status = VALUES(Status),
            Processed_Timestamp = VALUES(Processed_Timestamp)
            """

            # Get event timestamp from message or use current time
            event_timestamp = message.get("EVENT_TIMESTAMP", "")
            if not event_timestamp:
                event_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Store the entire Kafka message as JSON in Kafka_Event column
            kafka_event_json = json.dumps(message)

            with self._connection.cursor() as cursor:
                cursor.execute(insert_sql, (
                    message_id,
                    kafka_event_json,
                    message.get("EVENT_TYPE", ""),
                    message.get("ST_BOM_DOC_PATH", ""),
                    message.get("ST_BOM_FILE_NAME", ""),
                    event_timestamp,
                    None,  # Processed_Timestamp is null initially
                    TransferStatus.CREATED
                ))

            self._logger.log_info(
                message=f"Inserted record with status 'Created' for message: {message_id}",
                status=Status.Running,
                correlation_id=message_id,
                source="SingleStore"
            )
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to insert record: {e}",
                status=Status.Failed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return False

    def update_processed(self, message_id: str) -> bool:
        """
        Update record status to 'Processed' with processed timestamp.

        Args:
            message_id: The MESSAGE_ID

        Returns:
            True if update successful
        """
        if not self._ensure_connection():
            return False

        try:
            update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET Status = %s, Processed_Timestamp = %s
            WHERE MESSAGE_ID = %s
            """

            processed_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with self._connection.cursor() as cursor:
                cursor.execute(update_sql, (
                    TransferStatus.PROCESSED,
                    processed_timestamp,
                    message_id
                ))

            self._logger.log_info(
                message=f"Updated record to 'Processed' for message: {message_id}",
                status=Status.Completed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to update processed status: {e}",
                status=Status.Failed,
                correlation_id=message_id,
                source="SingleStore"
            )
            return False

    def update_failed(self, message_id: str, error_message: str = None) -> bool:
        """
        Update record status to 'Failed' with processed timestamp.

        Args:
            message_id: The MESSAGE_ID
            error_message: Optional error description (logged but not stored in table)

        Returns:
            True if update successful
        """
        if not self._ensure_connection():
            return False

        try:
            update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET Status = %s, Processed_Timestamp = %s
            WHERE MESSAGE_ID = %s
            """

            processed_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with self._connection.cursor() as cursor:
                cursor.execute(update_sql, (
                    TransferStatus.FAILED,
                    processed_timestamp,
                    message_id
                ))

            self._logger.log_info(
                message=f"Updated record to 'Failed' for message: {message_id}. Error: {error_message}",
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

    def _update_status(self, message_id: str, status: str) -> bool:
        """Generic status update with processed timestamp."""
        if not self._ensure_connection():
            return False

        try:
            update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET Status = %s, Processed_Timestamp = %s
            WHERE MESSAGE_ID = %s
            """

            processed_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with self._connection.cursor() as cursor:
                cursor.execute(update_sql, (status, processed_timestamp, message_id))

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
            WHERE MESSAGE_ID = %s
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
            WHERE Status = %s
            ORDER BY EVENT_TIMESTAMP DESC
            LIMIT %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(select_sql, (TransferStatus.FAILED, limit))
                return cursor.fetchall()

        except Exception:
            return []

    def get_created_records(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get records with 'Created' status that need processing."""
        if not self._ensure_connection():
            return []

        try:
            select_sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE Status = %s
            ORDER BY EVENT_TIMESTAMP ASC
            LIMIT %s
            """

            with self._connection.cursor() as cursor:
                cursor.execute(select_sql, (TransferStatus.CREATED, limit))
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
