from typing import Any, Callable
from itertools import chain

from pydantic import BaseModel

from lightningdb.rw.read_df import iterrows
from lightningdb.rw.write_df import WriteDF

import multiprocessing


# FlatMap class applies a function to each input row, potentially producing multiple output rows.
# It reads input files, processes each row with the given function, and writes the results to output files.
#
# Attributes:
#   fn: A function that takes a single row and returns a list of rows.
#   avro_schema: The schema for the output data.
class FlatMap(BaseModel):
    fn: Callable[[Any], list[Any]]
    avro_schema: Any

    def run(self, input_files: list[str], output_dir: str) -> list[str]:
        writer = WriteDF(output_dir, self.avro_schema)
        # Apply the function to each input row using multiprocessing
        # This allows for parallel processing of input rows, potentially
        # improving performance for CPU-bound operations
        with multiprocessing.Pool() as pool:
            for results in pool.imap_unordered(
                self.fn, iterrows(input_files), chunksize=100
            ):
                for result in results:
                    writer.append(result)
        writer.close()
        return writer.files
