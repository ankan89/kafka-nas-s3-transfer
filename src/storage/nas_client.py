"""
NAS client for PVC-based file access.
Reads files from a mounted PVC instead of using SMB protocol.
"""

import os
from typing import Tuple

from src.config_loader import ConfigLoader
from src.logging_config import CdpLogger
from scm_utilities.Constant.technical import Status


class NasClient:
    """
    Client for accessing files on NAS via mounted PVC.
    Replaces SMB/CIFS protocol with direct file system access.
    """

    DEFAULT_PVC_MOUNT_PATH = "/data/pvc"

    def __init__(self):
        self._config = ConfigLoader()
        self._logger = CdpLogger.get_instance()
        self._pvc_mount_path = os.environ.get("PVC_MOUNT_PATH", self.DEFAULT_PVC_MOUNT_PATH)

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

    def _transform_nas_to_pvc_path(self, nas_path: str) -> str:
        """
        Transform NAS path to PVC mount path.

        NAS Path:  \\\\server\\share\\Outbound_Data\\file.json
        PVC Path:  /data/pvc/Outbound_Data/file.json

        Args:
            nas_path: Full NAS path

        Returns:
            Corresponding PVC file path
        """
        # Parse to get the relative path (after server and share)
        _, _, relative_path = self._parse_nas_path(nas_path)

        # Convert Windows separators to Unix
        relative_path = relative_path.replace("\\", "/")

        # Combine with PVC mount path
        return os.path.join(self._pvc_mount_path, relative_path)

    def connect(self, server: str = None) -> bool:
        """
        Verify PVC mount exists.

        Args:
            server: Ignored (kept for backward compatibility)

        Returns:
            True if PVC mount directory exists
        """
        try:
            if os.path.isdir(self._pvc_mount_path):
                self._logger.log_info(
                    message=f"PVC mount verified at: {self._pvc_mount_path}",
                    status=Status.Running,
                    source="NAS"
                )
                return True
            else:
                self._logger.log_error(
                    message=f"PVC mount not found at: {self._pvc_mount_path}",
                    status=Status.Failed,
                    source="NAS"
                )
                return False

        except Exception as e:
            self._logger.log_error(
                message=f"Failed to verify PVC mount: {e}",
                status=Status.Failed,
                source="NAS"
            )
            return False

    def download_file(self, nas_path: str) -> bytes:
        """
        Read file from PVC mount.

        Args:
            nas_path: Full NAS path to the file

        Returns:
            File contents as bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If file read fails
        """
        try:
            pvc_path = self._transform_nas_to_pvc_path(nas_path)

            self._logger.log_info(
                message=f"Reading file from PVC: {pvc_path}",
                status=Status.Running,
                source="NAS"
            )

            if not os.path.isfile(pvc_path):
                self._logger.log_error(
                    message=f"File not found: {pvc_path}",
                    status=Status.Failed,
                    source="NAS"
                )
                raise FileNotFoundError(f"File not found: {pvc_path}")

            with open(pvc_path, "rb") as f:
                content = f.read()

            self._logger.log_info(
                message=f"Read {len(content)} bytes from {pvc_path}",
                status=Status.Completed,
                source="NAS"
            )

            return content

        except FileNotFoundError:
            raise

        except Exception as e:
            self._logger.log_error(
                message=f"Error reading file {nas_path}: {e}",
                status=Status.Failed,
                source="NAS"
            )
            raise

    def file_exists(self, nas_path: str) -> bool:
        """
        Check if file exists on PVC mount.

        Args:
            nas_path: Full NAS path to check

        Returns:
            True if file exists
        """
        try:
            pvc_path = self._transform_nas_to_pvc_path(nas_path)
            return os.path.isfile(pvc_path)

        except Exception:
            return False

    def close(self):
        """No-op (no connection to close with PVC mount)."""
        self._logger.log_info(
            message="NAS client closed (PVC mode)",
            status=Status.Completed,
            source="NAS"
        )
