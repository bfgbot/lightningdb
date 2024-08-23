from fastavro import parse_schema
from fastavro.write import Writer

from lightningdb.rw.write_wrapper import WriteWrapper


class WriteAvro:
    """
    A class for writing Avro-formatted data to a specified URI.
    Uses WriteWrapper for handling local or S3 destinations and fastavro for Avro encoding.
    """

    def __init__(self, uri, schema) -> None:
        """
        Initialize the WriteAvro object.

        :param uri: The destination URI (local path or S3 URI)
        :param schema: The Avro schema for the data to be written
        """
        self._bytes_writer = WriteWrapper(uri)
        # Use zstandard codec for compression
        self._avro_writer = Writer(
            self._bytes_writer, schema=parse_schema(schema), codec="zstandard"
        )

    def append(self, row) -> None:
        self._avro_writer.write(row)

    def close(self) -> None:
        """
        Finalize the Avro file and close the underlying WriteWrapper.
        """
        self._avro_writer.flush()
        self._bytes_writer.close()

    def size(self) -> int:
        """
        Return the current size of the written data in bytes.
        """
        return self._bytes_writer.size
