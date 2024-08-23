from typing import Any

from pydantic import BaseModel

from lightningdb.rw.write_df import WriteDF


# Const class generates a constant dataset by writing a predefined list of items to output files.
# It ignores input files and always produces the same output based on its 'items' attribute.
# This is useful for creating static datasets or injecting constant data into a processing pipeline.
class Const(BaseModel, extra="forbid"):
    items: list[Any]
    avro_schema: Any

    def run(self, input_files: list[str], output_dir: str) -> list[str]:
        writer = WriteDF(output_dir, self.avro_schema)
        for item in self.items:
            writer.append(item)
        writer.close()
        return writer.files
