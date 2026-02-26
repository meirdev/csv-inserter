# csv-inserter

`csv-inserter` is a lightweight ingestion daemon that automatically inserts CSV files into ClickHouse.

Instead of manually running `clickhouse-client` for every CSV file and opening a new connection each time, `csv-inserter` lets you build a simple, reliable pipeline:

> Drop a CSV file into a directory → it gets inserted into ClickHouse automatically.

## Example Workflow

```bash
# Start the service
csv-inserter --watch-dir /data/incoming \
             --table events \
             --database analytics \
             --clickhouse-url http://localhost:8123
```

Now:

```bash
mv batch_01.csv /data/incoming/
```

And it gets inserted automatically.
