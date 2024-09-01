import json
import os
import sqlite3
from typing import Optional

schema = """
create table if not exists df(
    name text not null,
    part integer not null,
    files json not null,
    primary key(name, part)
);
"""


class LightningCtx:
    """
    LightningCtx manages the context for working with dataframes (df) in LightningDB.

    A df represents a table of data that is partitioned into parts. Each part consists
    of one or more files in Avro format, all sharing the same schema. Parts are created
    or updated atomically, enabling efficient sharded parallel processing of the
    full dataframe.

    This class provides methods to create, update, delete, and retrieve information
    about dataframes and their parts, using an SQLite database for metadata storage.
    """

    db: sqlite3.Connection
    repodir: str

    def __init__(self, dbname: str, repodir: str):
        self.db = sqlite3.connect(dbname)
        self.db.executescript(schema)
        self.repodir = repodir
        if not self.repodir.startswith("s3://"):
            os.makedirs(self.repodir, exist_ok=True)

    def new_df(self, name: str, part: int, files: list[str]) -> None:
        """
        Create a new dataframe with the given name and part.
        """
        self.db.execute(
            "insert into df(name, part, files) values(?, ?, ?) on conflict(name, part)"
            " do update set files = excluded.files",
            (name, part, json.dumps(files)),
        )
        self.db.commit()

    def drop_df(self, name: str, part: Optional[int] = None) -> None:
        """
        Delete a dataframe with the given name and part.
        """
        if part is None:
            self.db.execute("delete from df where name = ?", (name,))
        else:
            self.db.execute("delete from df where name = ? and part = ?", (name, part))
        self.db.commit()

    def get_files(self, name: str, part: int) -> list[str]:
        """
        Get the files for a dataframe with the given name and part.
        """
        cur = self.db.execute(
            "select files from df where name = ? and part = ?", (name, part)
        )
        ret = cur.fetchone()
        if ret is None:
            return []
        (f,) = ret
        return json.loads(f)
