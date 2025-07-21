import base64
import io
import tarfile
from pathlib import Path
from typing import Optional, List, Tuple

from loguru import logger
from pydantic import BaseModel


class TarGzFile(BaseModel):
    filename: str
    codec: str = "tar.gz+base64"
    content: str  # base-64 string
    count: int  # number of files in the archive


class TarGz:
    @staticmethod
    def _get_size(buf: io.BytesIO) -> str:
        return f"{buf.getbuffer().nbytes / 1024:,.2f} KB"


    @staticmethod
    def compress_files(
            *,
            paths: Optional[List[str]] = None,
            files: Optional[List[Tuple[str, bytes]]] = None,
            archive_name: str = "archive",
            log_prefix: str = ""
    ) -> TarGzFile:
        """
        Build a tar.gz archive in memory from disk paths and/or in-memory byte blobs.

        Parameters
        ----------
        paths
            List of file paths on disk to include.
        files
            List of tuples ``(filename, data_bytes)`` for in-memory content.
        archive_name
            Name to store in the result (does **not** touch the filesystem).
        log_prefix
            Prefix for logging messages.

        Returns
        -------
        TarGzFile
            Pydantic model ready to push through Redis.
        """
        if not paths and not files:
            raise ValueError("Provide at least one of 'paths' or 'files'")

        buf = io.BytesIO()

        # Build tar.gz archive entirely in RAM
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            # Add disk files
            if paths:
                for p in paths:
                    p_path = Path(p)
                    tar.add(p_path, arcname=p_path.name)

            # Add in-memory files
            if files:
                for name, data in files:
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    tar.addfile(info, io.BytesIO(data))

            logger.info(
                f"[{log_prefix}] Created archive {archive_name}, "
                f"{len(paths) if paths else 0} paths, "
                f"{len(files) if files else 0} files, "
                f"Archive size: {TarGz._get_size(buf)}"
            )

        # Prepare output
        encoded = base64.b64encode(buf.getvalue()).decode()
        total = (len(paths) if paths else 0) + (len(files) if files else 0)

        return TarGzFile(
            filename=f"{archive_name}.tar.gz",
            content=encoded,
            count=total,
        )

    @staticmethod
    def extract(archive_bytes: str | bytes, codec: str, save_dir: str, log_prefix: str = "") -> None:
        """
        Extract a tar.gz archive from base64-encoded bytes and save to the specified directory.
        :param archive_bytes: archive bytes in string or bytes
        :param codec: codec used for the archive, expected "tar.gz+base64"
        :param save_dir: directory to save the extracted files
        :param log_prefix: prefix for logging messages
        :return:
        """
        if codec != "tar.gz+base64":
            logger.error(f"[{log_prefix}]Unknown codec, skipping")
            return

        decoded_archive = base64.b64decode(
            archive_bytes.decode('utf-8') if isinstance(archive_bytes, bytes) else archive_bytes
        )

        archive_buf = io.BytesIO(decoded_archive)
        logger.info(f"[{log_prefix}] Archive size: {TarGz._get_size(archive_buf)}")
        # ── unpack ────────────────────────────────────────────────────────
        with tarfile.open(fileobj=archive_buf, mode="r:gz") as tar:
            tar.extractall(path=save_dir)
        logger.info(f"[{log_prefix}] restored files to {save_dir}")
