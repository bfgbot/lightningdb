import os
import tempfile

from lightningdb.rw.s3utils import upload_file


class WriteWrapper:
    """
    A wrapper class for writing data to either a local file or an S3 bucket.
    Provides a unified interface for writing, regardless of the destination.
    Includes self.size attribute for tracking the total number of bytes written.
    Implements context manager protocol for safe resource management.
    """

    def __init__(self, uri):
        self._uri = uri
        self.size = 0

        if uri.startswith("s3://"):
            # For S3, create a temporary local file
            self._fp = tempfile.NamedTemporaryFile()
        else:
            dir = os.path.dirname(uri)
            os.makedirs(dir, exist_ok=True)
            self._fp = open(uri, "wb")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def write(self, buffer, /) -> int:
        n = self._fp.write(buffer)
        self.size += n
        return n

    def close(self):
        if self._uri.startswith("s3://"):
            # For S3, upload the temporary file to the specified S3 location
            self._fp.seek(0)
            upload_file(self._fp, self._uri)
        self._fp.close()

    def seekable(self):
        return False

    def flush(self):
        self._fp.flush()

    def fileno(self):
        return self._fp.fileno()
