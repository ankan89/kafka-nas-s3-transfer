"""
S3 client for Dell ECS (S3-compatible storage).
Handles file uploads to S3 bucket with proper path structure.
"""

import os
from datetime import datetime
from typing import Optional, Dict, Any
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config_loader import ConfigLoader
from src.logging_config import CdpLogger
from scm_utilities.Constant.technical import Status


class S3Client:
    """
    Client for uploading files to S3-compatible storage (Dell ECS).
    """

    def __init__(self):
        self._config = ConfigLoader()
        self._logger = CdpLogger.get_instance()

        # Get S3 configuration (Vault placeholders already resolved)
        self._s3_config = self._config.get("s3", {})
        self._client: Optional[boto3.client] = None

        # Set AWS checksum environment variables
        os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
        os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"

    def connect(self) -> bool:
        """
        Initialize S3 client with custom endpoint for Dell ECS.

        Returns:
            True if connection successful
        """
        try:
            endpoint_url = self._s3_config.get("end_point", "")
            access_key = self._s3_config.get("access_key", "")
            secret_key = self._s3_config.get("secret_key", "")
            region = self._s3_config.get("region", "us-east-1")

            if not all([endpoint_url, access_key, secret_key]):
                self._logger.log_error(
                    message="Missing required S3 configuration",
                    status=Status.Failed,
                    source="S3"
                )
                return False

            # Configure boto3 client
            boto_config = Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'},
                retries={
                    'max_attempts': 3,
                    'mode': 'adaptive'
                }
            )

            self._client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
                config=boto_config
            )

            # Test connection by listing buckets (optional)
            # self._client.list_buckets()

            self._logger.log_info(
                message=f"Connected to S3: {endpoint_url}",
                status=Status.Running,
                source="S3"
            )
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to connect to S3: {e}",
                status=Status.Failed,
                source="S3"
            )
            return False

    def build_s3_path(self, message: Dict[str, Any]) -> str:
        """
        Build S3 path based on message metadata.

        Path structure:
        {folder_name}/{EVENT_TYPE}/{ST_BOM_TYPE}/{IS_COSTED}/{OLP_PHASE}/{YYYY}/{MM}/{DD}/{FILE_NAME}

        Args:
            message: Kafka message with file metadata

        Returns:
            S3 object key (path)
        """
        folder_name = self._s3_config.get("folder_name", "dp03")

        event_type = message.get("EVENT_TYPE", "UNKNOWN")
        st_bom_type = message.get("ST_BOM_TYPE", "UNKNOWN")
        is_costed = message.get("IS_COSTED", "UNKNOWN")
        olp_phase = message.get("OLP_PHASE", "UNKNOWN")
        file_name = message.get("ST_BOM_FILE_NAME", "")

        # Parse timestamp for date folder structure
        timestamp_str = message.get("EVENT_TIMESTAMP", "")
        try:
            if timestamp_str:
                # Expected format: "2025-11-13 HH:MM:SS"
                dt = datetime.strptime(timestamp_str.split()[0], "%Y-%m-%d")
            else:
                dt = datetime.now()
        except ValueError:
            dt = datetime.now()

        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        # Build path
        s3_path = f"{folder_name}/{event_type}/{st_bom_type}/{is_costed}/{olp_phase}/{year}/{month}/{day}/{file_name}"

        return s3_path

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ClientError, ConnectionError))
    )
    def upload_file(self, file_data: bytes, s3_path: str, content_type: str = None) -> str:
        """
        Upload file to S3.

        Args:
            file_data: File contents as bytes
            s3_path: S3 object key (path)
            content_type: Optional content type

        Returns:
            Full S3 URI of uploaded file

        Raises:
            ClientError: If upload fails
        """
        if not self._client:
            if not self.connect():
                raise ConnectionError("Failed to connect to S3")

        bucket_name = self._s3_config.get("bucket_name", "SDS")

        try:
            self._logger.log_info(
                message=f"Uploading to S3: s3://{bucket_name}/{s3_path}",
                status=Status.Running,
                source="S3"
            )

            # Determine content type
            if content_type is None:
                if s3_path.lower().endswith('.json'):
                    content_type = 'application/json'
                elif s3_path.lower().endswith('.xml'):
                    content_type = 'application/xml'
                elif s3_path.lower().endswith('.csv'):
                    content_type = 'text/csv'
                else:
                    content_type = 'application/octet-stream'

            # Upload file
            extra_args = {
                'ContentType': content_type
            }

            self._client.put_object(
                Bucket=bucket_name,
                Key=s3_path,
                Body=file_data,
                **extra_args
            )

            s3_uri = f"s3://{bucket_name}/{s3_path}"

            self._logger.log_info(
                message=f"Successfully uploaded to: {s3_uri}",
                status=Status.Completed,
                source="S3"
            )

            return s3_uri

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            self._logger.log_error(
                message=f"S3 upload failed ({error_code}): {e}",
                status=Status.Failed,
                source="S3"
            )
            raise

        except Exception as e:
            self._logger.log_error(
                message=f"Error uploading to S3: {e}",
                status=Status.Failed,
                source="S3"
            )
            raise

    def file_exists(self, s3_path: str) -> bool:
        """
        Check if file exists in S3.

        Args:
            s3_path: S3 object key

        Returns:
            True if file exists
        """
        if not self._client:
            if not self.connect():
                return False

        bucket_name = self._s3_config.get("bucket_name", "SDS")

        try:
            self._client.head_object(Bucket=bucket_name, Key=s3_path)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def delete_file(self, s3_path: str) -> bool:
        """
        Delete file from S3.

        Args:
            s3_path: S3 object key

        Returns:
            True if deletion successful
        """
        if not self._client:
            if not self.connect():
                return False

        bucket_name = self._s3_config.get("bucket_name", "SDS")

        try:
            self._client.delete_object(Bucket=bucket_name, Key=s3_path)
            self._logger.log_info(
                message=f"Deleted S3 object: s3://{bucket_name}/{s3_path}",
                status=Status.Completed,
                source="S3"
            )
            return True

        except ClientError as e:
            self._logger.log_error(
                message=f"Failed to delete S3 object: {e}",
                status=Status.Failed,
                source="S3"
            )
            return False
