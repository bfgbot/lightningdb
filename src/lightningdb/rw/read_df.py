from typing import Any, Iterator

from fastavro import reader as fastavro_reader

from lightningdb.rw.read_wrapper import ReadWrapper


def iterrows(files: list[str]) -> Iterator[Any]:
    """
    Iterate over rows from multiple files.

    Args:
        files (list[str]): List of file paths or URIs to read from.

    Yields:
        Any: Each row from the files.
    """
    for file in files:
        bytes_reader = ReadWrapper(file)
        for row in fastavro_reader(bytes_reader):
            yield row
        bytes_reader.close()
