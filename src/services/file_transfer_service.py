"""
File Transfer Service - Main orchestration for NAS to S3 transfers.
Coordinates all components for transactional file transfer.
"""

import os
import tempfile
import shutil
from typing import Dict, Any

from src.logging_config import CdpLogger
from src.storage.nas_client import NasClient
from src.storage.s3_client import S3Client
from src.database.singlestore_client import SingleStoreClient, TransferStatus
from scm_utilities.Constant.technical import Status


class FileTransferService:
    """
    Orchestrates file transfer from NAS to S3.
    Ensures transactional behavior - only marks complete on success.
    """

    def __init__(self):
        self._logger = CdpLogger.get_instance()
        self._nas = NasClient()
        self._s3 = S3Client()
        self._db = SingleStoreClient()

        # Initialize connections
        self._initialize_connections()

    def _initialize_connections(self):
        """Initialize all client connections."""
        self._logger.log_info(
            message="Initializing file transfer service connections",
            status=Status.Running,
            source="FileTransfer"
        )

        # SingleStore connection for tracking
        try:
            self._db.connect()
        except Exception as e:
            self._logger.log_warning(
                message=f"SingleStore connection failed, continuing without tracking: {e}",
                status=Status.Running,
                source="FileTransfer"
            )

        # S3 connection will be established on first use
        # NAS connection will be established on first file access

    def process_message(self, message: Dict[str, Any], kafka_topic: str = None) -> bool:
        """
        Process a Kafka message and transfer file from NAS to S3.

        Workflow:
        1. Read Kafka message and extract data
        2. Insert record into NAS_File_Tracker with status 'Created' (stores entire Kafka msg)
        3. Search for file in NAS folder
        4. Copy file to S3
        5. Update status to 'Processed' or 'Failed'

        Args:
            message: Kafka message containing file metadata
            kafka_topic: The Kafka topic name (for logging only)

        Returns:
            True if processing successful (commit offset)
            False if processing failed (do not commit, will retry)
        """
        message_id = message.get("MESSAGE_ID")

        self._logger.log_info(
            message=f"Processing message: {message_id} from topic: {kafka_topic}",
            status=Status.Running,
            correlation_id=message_id,
            source="FileTransfer"
        )

        # Initialize temp_dir for cleanup in exception handler
        temp_dir = None

        try:
            # Step 1: Validate message
            if not self._validate_message(message):
                self._logger.log_error(
                    message=f"Invalid message format: {message_id}",
                    status=Status.Failed,
                    correlation_id=message_id,
                    source="FileTransfer"
                )
                # Invalid message - commit to skip (no point retrying)
                return True

            # Step 2: Insert record with status 'Created' into NAS_File_Tracker
            # The entire Kafka message is stored in Kafka_Event column
            if not self._db.insert_created(message):
                self._logger.log_warning(
                    message=f"Failed to insert record for message: {message_id}, continuing with transfer",
                    status=Status.Running,
                    correlation_id=message_id,
                    source="FileTransfer"
                )

            # Step 3: Build file paths
            nas_path = self._build_nas_path(message)
            s3_path = self._s3.build_s3_path(message)

            self._logger.log_info(
                message=f"Searching for file in NAS: {nas_path}",
                status=Status.Running,
                correlation_id=message_id,
                source="FileTransfer"
            )

            # Step 4: Copy file from NAS to local temp directory (without reading content into memory)
            local_file_path = None
            try:
                temp_dir = tempfile.mkdtemp(prefix="nas_transfer_")
                local_file_path = self._nas.copy_file_to_local(nas_path, temp_dir)
            except FileNotFoundError:
                self._logger.log_error(
                    message=f"File not found on NAS: {nas_path}",
                    status=Status.Failed,
                    correlation_id=message_id,
                    source="FileTransfer"
                )
                self._db.update_failed(message_id, f"File not found: {nas_path}")
                # Clean up temp dir if created
                if temp_dir and os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                # File doesn't exist - commit to skip (no point retrying)
                return True

            self._logger.log_info(
                message=f"Uploading file to S3: {s3_path}",
                status=Status.Running,
                correlation_id=message_id,
                source="FileTransfer"
            )

            # Step 5: Upload to S3 from local file (streaming upload)
            try:
                s3_uri = self._s3.upload_from_file(local_file_path, s3_path)
            finally:
                # Clean up local temp file
                if temp_dir and os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    self._logger.log_info(
                        message=f"Cleaned up temp directory: {temp_dir}",
                        status=Status.Running,
                        correlation_id=message_id,
                        source="FileTransfer"
                    )

            # Step 6: Update status to 'Processed' in NAS_File_Tracker
            self._db.update_processed(message_id)

            self._logger.log_info(
                message=f"Transfer complete: {message_id} -> {s3_uri}",
                status=Status.Completed,
                correlation_id=message_id,
                source="FileTransfer"
            )

            # Success - commit offset and wait for next message
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Transfer failed for {message_id}: {str(e)}",
                status=Status.Failed,
                correlation_id=message_id,
                source="FileTransfer"
            )

            # Clean up temp directory if it exists
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

            # Update status to 'Failed' in NAS_File_Tracker
            self._db.update_failed(message_id, str(e))

            # Failure - DO NOT commit offset (will retry)
            return False

    def _validate_message(self, message: Dict[str, Any]) -> bool:
        """
        Validate message has required fields.

        Args:
            message: Kafka message to validate

        Returns:
            True if valid
        """
        required_fields = [
            "MESSAGE_ID",
            "EVENT_TYPE",
            "ST_BOM_DOC_PATH",
            "ST_BOM_FILE_NAME"
        ]

        for field in required_fields:
            if not message.get(field):
                self._logger.log_warning(
                    message=f"Missing required field: {field}",
                    status=Status.Running,
                    source="FileTransfer"
                )
                return False

        return True

    def _build_nas_path(self, message: Dict[str, Any]) -> str:
        """
        Build full NAS path from message.

        Args:
            message: Kafka message with path components

        Returns:
            Full NAS path
        """
        doc_path = message.get("ST_BOM_DOC_PATH", "")
        file_name = message.get("ST_BOM_FILE_NAME", "")

        # Normalize path separators
        doc_path = doc_path.replace("/", "\\")
        if not doc_path.endswith("\\"):
            doc_path += "\\"

        return f"{doc_path}{file_name}"

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all components.

        Returns:
            Health status dictionary
        """
        health = {
            "healthy": True,
            "components": {}
        }

        # Check SingleStore
        try:
            db_ok = self._db._ensure_connection()
            health["components"]["singlestore"] = "healthy" if db_ok else "unhealthy"
        except Exception:
            health["components"]["singlestore"] = "unhealthy"

        # Check S3
        try:
            s3_ok = self._s3.connect()
            health["components"]["s3"] = "healthy" if s3_ok else "unhealthy"
        except Exception:
            health["components"]["s3"] = "unhealthy"

        # Overall health - S3 is critical
        health["healthy"] = health["components"].get("s3") == "healthy"

        return health

    def close(self):
        """Close all client connections."""
        self._logger.log_info(
            message="Shutting down file transfer service",
            status=Status.Running,
            source="FileTransfer"
        )

        try:
            self._nas.close()
        except Exception:
            pass

        try:
            self._db.close()
        except Exception:
            pass

        self._logger.log_info(
            message="File transfer service shutdown complete",
            status=Status.Completed,
            source="FileTransfer"
        )
