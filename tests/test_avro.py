from lightningdb.rw.read_df import iterrows
from lightningdb.rw.write_avro import WriteAvro


def test_write_and_read_avro():
    schema = {
        "type": "record",
        "name": "User",
        "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
    }

    rows = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    writer = WriteAvro("/tmp/a.avro", schema)
    for row in rows:
        writer.append(row)
    writer.close()

    received = list(iterrows(["/tmp/a.avro"]))
    print(received)
    assert received == rows
