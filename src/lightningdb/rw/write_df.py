import os
import random
import string
from typing import Any

from lightningdb.rw.write_avro import WriteAvro

# SPLIT_SIZE defines the target size for each file slice in bytes
# When the current file size exceeds this value, a new slice is created
# The actual file size may be slightly larger than SPLIT_SIZE
# This approach helps in managing large datasets by splitting them into more manageable chunks
SPLIT_SIZE = 100_000_000


def new_file() -> str:
    """
    Generate a random filename with a 5-character alphanumeric string and '.avro' extension.

    Returns:
        str: A randomly generated filename ending with '.avro'
    """
    charset = string.ascii_letters + string.digits
    random_string = "".join(random.choice(charset) for _ in range(5))
    return f"{random_string}.avro"


class WriteDF:
    """
    A class for writing dataframes to Avro files, with support for splitting large datasets.
    """

    def __init__(self, dir: str, avro_schema: Any):
        self.dir = dir
        self.avro_schema = avro_schema

        self.files = []  # List to store names of created files
        self.writer = None  # Current Avro writer

    def _new_slice(self) -> None:
        """
        Creates a new Avro writer for the next file slice.
        """
        assert self.writer is None

        file = new_file()
        self.files.append(file)

        fullpath = os.path.join(self.dir, file)
        self.writer = WriteAvro(fullpath, self.avro_schema)

    def append(self, row: Any) -> None:
        """
        Appends a row to the current Avro file, creating a new file if necessary.
        """
        if self.writer is None:
            self._new_slice()

        self.writer.append(row)

        # If the current file exceeds the size limit, finalize it and prepare for a new one
        if self.writer.size() > SPLIT_SIZE:
            self.close()

    def close(self) -> None:
        """
        Closes the current Avro writer and finalizes the last slice.
        """
        if self.writer is not None:
            self.writer.close()
            self.writer = None
