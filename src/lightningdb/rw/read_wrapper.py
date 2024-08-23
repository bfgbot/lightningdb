import tempfile

from lightningdb.rw.s3utils import download_file


class ReadWrapper:
    """
    A wrapper class that provides a unified interface for reading files from both local storage and S3 buckets.

    This class abstracts the differences between reading from local files and S3, allowing the client code
    to interact with both types of storage in the same way.
    """

    def __init__(self, uri: str) -> None:
        if uri.startswith("s3://"):
            # For S3 URIs, download the file to a temporary local file
            # The temporary file is automatically deleted when closed
            self.fp = tempfile.NamedTemporaryFile()
            download_file(uri, self.fp)
            self.fp.seek(0)  # Reset file pointer to the beginning after download
        else:
            # For local files, simply open the file in binary read mode
            self.fp = open(uri, "rb")

    def read(self, size: int = -1) -> bytes:
        """
        Read data from the file.

        Args:
            size (int): The number of bytes to read. If negative or omitted, read until EOF.

        Returns:
            bytes: The data read from the file.
        """
        return self.fp.read(size)

    def close(self) -> None:
        """
        Close the file pointer.

        For S3 files, this also ensures the deletion of the temporary local file.
        """
        self.fp.close()
