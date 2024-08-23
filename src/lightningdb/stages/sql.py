import os
import subprocess

from pydantic import BaseModel

from lightningdb.rw.s3utils import parse_s3_uri
from lightningdb.rw.write_df import new_file
from lightningdb.rw.write_wrapper import WriteWrapper


# ClickHouse requires S3 URIs to be presented as HTTP URIs for direct access
def format_s3_uri(uri: str) -> str:
    if not uri.startswith("s3://"):
        return uri
    bucket, key = parse_s3_uri(uri)
    return f"http://{bucket}.s3.amazonaws.com/{key}"


# This function runs ClickHouse locally to process data from one or more input
# files (local or S3) and outputs to a single file.
# It executes SQL queries on the input data and handles the ClickHouse command execution.
def run_clickhouse(sql: str, output_file: str, input_files: list[str]):
    input_files = [format_s3_uri(f) for f in input_files]

    if len(input_files) == 1:
        input_str = input_files[0]
    elif len(input_files) > 1:
        input_str = "{" + ",".join(input_files) + "}"
    else:
        assert False, "No input files"

    if "http://" in input_str:
        view_query = f"create view input as select * from s3('{input_str}')"
    else:
        view_query = f"create view input as select * from file('{input_str}')"

    try:
        cmd = [
            "/tmp/clickhouse",  # Clickhouse binary is hardcoded
            "--query",
            view_query,
            "--query",
            sql,
            "--output_format_avro_codec",
            "zstd",
            "--output-format",
            "avro",
        ]
        with WriteWrapper(output_file) as f:
            subprocess.run(
                cmd,
                check=True,
                text=True,
                stdout=f,
                stderr=subprocess.PIPE,
            )
    except subprocess.CalledProcessError as e:
        print(e)
        raise e


# The Sql class executes SQL queries on input files using ClickHouse.
# It processes the input data, runs the specified SQL query, and outputs the result to a single file.
# This class is useful for performing complex data transformations using SQL.
class Sql(BaseModel):
    sql: str

    def run(self, input_files: list[str], output_dir: str) -> list[str]:
        output_file = new_file()
        output_fullpath = os.path.join(output_dir, output_file)
        run_clickhouse(self.sql, output_fullpath, input_files)
        return [output_file]
