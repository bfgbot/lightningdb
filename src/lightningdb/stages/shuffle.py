from typing import Any

from pydantic import BaseModel

from lightningdb.rw.read_df import iterrows
from lightningdb.rw.write_df import WriteDF


def to_part(row: dict, key: str, nparts: int) -> int:
    return hash(str(row[key])) % nparts


# The Shuffle class redistributes data across multiple partitions based on a specified key.
# It reads input files, assigns each row to a partition using a hash function,
# and writes the rows to their respective output files.
# This is useful for balancing data distribution or preparing for parallel processing.
class Shuffle(BaseModel):
    nparts: int
    key: str
    avro_schema: Any

    def runall(self, input_pfiles: list[list[str]], output_dir: str) -> list[list[str]]:
        input_files = [file for files in input_pfiles for file in files]
        writers = [WriteDF(output_dir, self.avro_schema) for _ in range(self.nparts)]

        for row in iterrows(input_files):
            part = to_part(row, self.key, self.nparts)
            writers[part].append(row)
        for writer in writers:
            writer.close()
        return [writer.files for writer in writers]
