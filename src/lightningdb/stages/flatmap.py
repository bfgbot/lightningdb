from typing import Any, Callable

from pydantic import BaseModel

from lightningdb.rw.read_df import iterrows
from lightningdb.rw.write_df import WriteDF


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
        for row in iterrows(input_files):
            for row in self.fn(row):
                writer.append(row)
        writer.close()
        return writer.files
