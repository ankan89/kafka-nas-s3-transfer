"""
NAS client for SMB/CIFS file access.
Uses smbprotocol for connecting to Windows network shares.
"""

import io
from typing import Optional, Tuple
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateDisposition, FilePipePrinterAccessMask, ImpersonationLevel
from smbprotocol.file_info import FileAttributes
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config_loader import ConfigLoader
from src.secrets import get_nas_credentials
from src.logging_config import CdpLogger
from scm_utilities.Constant.technical import Status


class NasClient:
    """
    Client for accessing files on NAS via SMB/CIFS protocol.
    """

    def __init__(self):
        self._config = ConfigLoader()
        self._logger = CdpLogger.get_instance()
        self._nas_credentials = get_nas_credentials()

        # Get NAS configuration from watchdog config
        watchdog_config = self._config.get("watchdog", [{}])[0]
        self._nas_path = watchdog_config.get("nas_path", "")

        self._connection: Optional[Connection] = None
        self._session: Optional[Session] = None

    def _parse_nas_path(self, full_path: str) -> Tuple[str, str, str]:
        """
        Parse NAS path into server, share, and file path components.

        Args:
            full_path: Full NAS path (e.g., \\\\server\\share\\folder\\file.txt)

        Returns:
            Tuple of (server, share, file_path)
        """
        # Normalize path separators
        path = full_path.replace("\\\\", "\\").replace("/", "\\")
        if path.startswith("\\"):
            path = path[1:]

        parts = path.split("\\")
        if len(parts) < 3:
            raise ValueError(f"Invalid NAS path format: {full_path}")

        server = parts[0]
        share = parts[1]
        file_path = "\\".join(parts[2:]) if len(parts) > 2 else ""

        return server, share, file_path

    def connect(self, server: str) -> bool:
        """
        Establish connection to NAS server.

        Args:
            server: NAS server hostname or IP

        Returns:
            True if connection successful
        """
        try:
            credentials = self._nas_credentials.get_credentials()

            if not self._nas_credentials.validate():
                self._logger.log_error(
                    message="NAS credentials not configured",
                    status=Status.Failed,
                    source="NAS"
                )
                return False

            # Create connection
            self._connection = Connection(
                uuid=None,
                server_name=server,
                port=445
            )
            self._connection.connect()

            # Create session with authentication
            username = credentials["username"]
            if "\\" in username:
                domain, username = username.split("\\", 1)
            else:
                domain = credentials.get("domain", "")

            self._session = Session(
                connection=self._connection,
                username=username,
                password=credentials["password"],
                require_encryption=False
            )
            self._session.connect()

            self._logger.log_info(
                message=f"Connected to NAS server: {server}",
                status=Status.Running,
                source="NAS"
            )
            return True

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to connect to NAS server {server}: {e}",
                status=Status.Failed,
                source="NAS"
            )
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    def download_file(self, nas_path: str) -> bytes:
        """
        Download file from NAS.

        Args:
            nas_path: Full NAS path to the file

        Returns:
            File contents as bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            ConnectionError: If connection fails
        """
        try:
            server, share, file_path = self._parse_nas_path(nas_path)

            # Connect if not already connected
            if not self._session:
                if not self.connect(server):
                    raise ConnectionError(f"Failed to connect to NAS server: {server}")

            self._logger.log_info(
                message=f"Downloading file: {file_path} from share: {share}",
                status=Status.Running,
                source="NAS"
            )

            # Connect to share
            tree = TreeConnect(
                session=self._session,
                share_name=f"\\\\{server}\\{share}"
            )
            tree.connect()

            try:
                # Open file for reading
                file_open = Open(tree=tree, file_name=file_path)
                file_open.create(
                    impersonation_level=ImpersonationLevel.Impersonation,
                    desired_access=FilePipePrinterAccessMask.GENERIC_READ,
                    file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
                    create_disposition=CreateDisposition.FILE_OPEN
                )

                try:
                    # Read file contents
                    file_size = file_open.end_of_file
                    content = file_open.read(0, file_size)

                    self._logger.log_info(
                        message=f"Downloaded {len(content)} bytes from {file_path}",
                        status=Status.Completed,
                        source="NAS"
                    )

                    return content

                finally:
                    file_open.close()

            finally:
                tree.disconnect()

        except FileNotFoundError:
            self._logger.log_error(
                message=f"File not found: {nas_path}",
                status=Status.Failed,
                source="NAS"
            )
            raise

        except Exception as e:
            self._logger.log_error(
                message=f"Error downloading file {nas_path}: {e}",
                status=Status.Failed,
                source="NAS"
            )
            raise

    def file_exists(self, nas_path: str) -> bool:
        """
        Check if file exists on NAS.

        Args:
            nas_path: Full NAS path to check

        Returns:
            True if file exists
        """
        try:
            server, share, file_path = self._parse_nas_path(nas_path)

            if not self._session:
                if not self.connect(server):
                    return False

            tree = TreeConnect(
                session=self._session,
                share_name=f"\\\\{server}\\{share}"
            )
            tree.connect()

            try:
                file_open = Open(tree=tree, file_name=file_path)
                file_open.create(
                    impersonation_level=ImpersonationLevel.Impersonation,
                    desired_access=FilePipePrinterAccessMask.GENERIC_READ,
                    file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
                    create_disposition=CreateDisposition.FILE_OPEN
                )
                file_open.close()
                return True

            except FileNotFoundError:
                return False

            finally:
                tree.disconnect()

        except Exception:
            return False

    def close(self):
        """Close NAS connection."""
        try:
            if self._session:
                self._session.disconnect()
                self._session = None

            if self._connection:
                self._connection.disconnect()
                self._connection = None

            self._logger.log_info(
                message="NAS connection closed",
                status=Status.Completed,
                source="NAS"
            )

        except Exception as e:
            self._logger.log_error(
                message=f"Error closing NAS connection: {e}",
                status=Status.Failed,
                source="NAS"
            )
