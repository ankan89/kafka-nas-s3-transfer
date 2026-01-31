"""
File Transfer Service - Main orchestration for NAS to S3 transfers.
Coordinates all components for transactional file transfer.
"""

from typing import Dict, Any

from src.logging_config import CdpLogger
from src.storage.nas_client import NasClient
from src.storage.s3_client import S3Client
from src.database.redis_client import RedisClient
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
        self._redis = RedisClient()
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

        # Redis is optional - continue if unavailable
        try:
            self._redis.connect()
        except Exception as e:
            self._logger.log_warning(
                message=f"Redis connection failed, continuing without deduplication: {e}",
                status=Status.Running,
                source="FileTransfer"
            )

        # SingleStore is optional - continue if unavailable
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

    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a Kafka message and transfer file from NAS to S3.

        This method implements transactional behavior:
        - Returns True only if transfer is successful (Kafka should commit)
        - Returns False if transfer fails (Kafka should NOT commit)

        Args:
            message: Kafka message containing file metadata

        Returns:
            True if processing successful (commit offset)
            False if processing failed (do not commit, will retry)
        """
        message_id = message.get("MESSAGE_ID", "unknown")

        self._logger.log_info(
            message=f"Processing message: {message_id}",
            status=Status.Running,
            correlation_id=message_id,
            source="FileTransfer"
        )

        try:
            # Step 1: Check for duplicate (Redis)
            if self._redis.is_processed(message_id):
                self._logger.log_info(
                    message=f"Duplicate message skipped: {message_id}",
                    status=Status.Completed,
                    correlation_id=message_id,
                    source="FileTransfer"
                )
                # Duplicate - commit offset to skip
                return True

            # Step 2: Validate message
            if not self._validate_message(message):
                self._logger.log_error(
                    message=f"Invalid message format: {message_id}",
                    status=Status.Failed,
                    correlation_id=message_id,
                    source="FileTransfer"
                )
                # Invalid message - commit to skip (no point retrying)
                return True

            # Step 3: Insert PENDING record to SingleStore
            self._db.insert_pending(message)
            self._db.update_in_progress(message_id)

            # Step 4: Build file paths
            nas_path = self._build_nas_path(message)
            s3_path = self._s3.build_s3_path(message)

            self._logger.log_info(
                message=f"Transferring file: {nas_path} -> {s3_path}",
                status=Status.Running,
                correlation_id=message_id,
                source="FileTransfer"
            )

            # Step 5: Download from NAS
            try:
                file_data = self._nas.download_file(nas_path)
            except FileNotFoundError:
                self._logger.log_error(
                    message=f"File not found on NAS: {nas_path}",
                    status=Status.Failed,
                    correlation_id=message_id,
                    source="FileTransfer"
                )
                self._db.update_failed(message_id, f"File not found: {nas_path}")
                # File doesn't exist - commit to skip (no point retrying)
                return True

            # Step 6: Upload to S3
            s3_uri = self._s3.upload_file(file_data, s3_path)

            # Step 7: Mark success in SingleStore
            self._db.update_success(message_id, s3_uri)

            # Step 8: Mark processed in Redis
            self._redis.mark_processed(message_id)

            self._logger.log_info(
                message=f"Transfer complete: {message_id} -> {s3_uri}",
                status=Status.Completed,
                correlation_id=message_id,
                source="FileTransfer"
            )

            # Success - commit offset
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Transfer failed for {message_id}: {str(e)}",
                status=Status.Failed,
                correlation_id=message_id,
                source="FileTransfer"
            )

            # Update database with failure
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

        # Check Redis
        try:
            redis_ok = self._redis._get_active_client() is not None
            health["components"]["redis"] = "healthy" if redis_ok else "unhealthy"
        except Exception:
            health["components"]["redis"] = "unhealthy"

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
            self._redis.close()
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
