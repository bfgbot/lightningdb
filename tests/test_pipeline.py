import os
import sqlite3

from lightningdb.df import LightningCtx
from lightningdb.pipeline import run_pipeline
from lightningdb.stages.const import Const
from lightningdb.stages.shuffle import Shuffle


def test_pipeline():
    schema = {
        "type": "record",
        "name": "User",
        "fields": [{"name": "a", "type": "int"}],
    }
    pipeline = [
        Const(items=[{"a": 1}, {"a": 2}, {"a": 3}], avro_schema=schema),
        Shuffle(nparts=2, key="a", avro_schema=schema),
    ]
    dbname = "/tmp/test.db"
    if os.path.exists(dbname):
        os.remove(dbname)
    ctx = LightningCtx(dbname, "/tmp/")
    run_pipeline(ctx, "test", pipeline, memorize=False)
    db = sqlite3.connect(dbname)
    cur = db.execute("select count(*) from df where name='test@1'")
    (result,) = cur.fetchone()
    assert result == 2
