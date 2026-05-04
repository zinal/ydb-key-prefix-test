# Read replica demonstration

Small Go utility that exercises **YDB read replicas** with **stale read-only** transactions: it dumps primary-key UUIDs to a binary key file, then runs concurrent point reads routed by **preferred node** hints derived from table partition bounds.

The target table path is fixed in code as `` `key_prefix_demo/main` `` (YQL). **DescribeTable** uses the full path under the database name from the DSN; data queries use the relative table name only.

---

## Program logic

### `dump-keys`

1. Opens a YDB connection and runs a **paged key-order scan** over `` `key_prefix_demo/main` ``.
2. Each page is a **Query Service** transaction (`db.Query().DoTx`) with **snapshot read-only** settings, matching the style of the [native query examples](https://github.com/ydb-platform/ydb-go-sdk/tree/master/examples/basic/native/query).
3. YQL uses `WHERE id > $last ORDER BY id LIMIT $limit` so ordering matches **YDB UUID cell order** (memcmp on the 16-byte representation), not canonical UUID string order.
4. Each row’s `id` is appended to the output file as **exactly 16 bytes** (RFC / `google/uuid` layout), with **no delimiters** between records.
5. Progress is logged about **every 10 seconds** (keys written so far).

### `test-reads`

1. **Describes** the same table via the **Table** API (`DescribeTable` with shard key bounds and partition stats) to build a **partition router**: exclusive upper bound per shard and leader node id for preferred-node hints.
2. **Samples** a keyset from the binary key file **without loading the whole file**: one forward pass with a logical cursor; each step skips a random number of whole records (mean roughly `nRecords / keyset`, capped), reads one UUID, advances; **EOF wraps to the start**. Deduplicates until `keyset` unique keys or an attempt limit. Progress is logged about every 10 seconds.
3. For each **outer round** (count = `-rounds`), builds a batch of up to `-batch` keys drawn at random from the keyset (sorted in YDB UUID order for the batch), splits keys by partition, and runs **one Query Service transaction per partition** in parallel (`StaleReadOnly`), with **`ydb.WithPreferredNodeID`** from a random key in that partition’s batch.
4. Execution progress (rounds completed, partition-queries, rows returned) is logged about **every 10 seconds**.

### Read replicas

Point reads use **stale read-only** isolation so they may be served from **read replicas** if the table is configured with `READ_REPLICAS_SETTINGS` (see `alter-main-read-replicas.sql` in this directory and the [YDB read replica docs](https://ydb.tech/docs/en/concepts/datamodel/table?version=v25.3#read_only_replicas)).

---

## Build

From this directory:

```bash
./build.sh                 # writes ./read-replica-demo
./build.sh ./bin/my-demo   # custom output path
```

Or: `go build -o read-replica-demo .`

---

## Usage

```text
<binary> dump-keys  -ydb <dsn> [-out file] [-page-size N]
<binary> test-reads -ydb <dsn> [-keys file] [-keyset N] [-batch N] [-rounds N] [-seed N]
```

| Mode | Flag | Default | Meaning |
|------|------|---------|--------|
| both | `-ydb` | (required) | YDB connection string |
| `dump-keys` | `-out` | `keys.bin` | Output path for 16-byte UUID records |
| `dump-keys` | `-page-size` | `1000` | Rows per `SELECT` page |
| `test-reads` | `-keys` | `keys.bin` | Key file from `dump-keys` |
| `test-reads` | `-keyset` | `10000` | Target number of unique sampled keys |
| `test-reads` | `-batch` | `32` | Max keys per partition batch |
| `test-reads` | `-rounds` | `100` | Outer rounds (each round issues partition queries) |
| `test-reads` | `-seed` | time-based | RNG seed for sampling and batch composition |

### Authentication and TLS

- Credentials follow **ydb-go-sdk-auth-environ** (e.g. `YDB_ANONYMOUS_CREDENTIALS=1`).
- If **both** `YDB_USER` and `YDB_PASSWORD` are set, **static** credentials are used instead of environ-based auth.
- For custom CA bundles, set **`YDB_SSL_ROOT_CERTIFICATES_FILE`**.

### Example

```bash
export YDB_SSL_ROOT_CERTIFICATES_FILE=/path/to/ca.crt
export YDB_USER=...
export YDB_PASSWORD=...

./read-replica-demo dump-keys \
  -ydb 'grpcs://host:2135/database' \
  -out keys.bin \
  -page-size 20000

./read-replica-demo test-reads \
  -ydb 'grpcs://host:2135/database' \
  -keys keys.bin \
  -keyset 10000 \
  -batch 32 \
  -rounds 100
```

---

## Notes

- **Key file**: size must be a multiple of **16** bytes. If `-keyset` exceeds the number of distinct UUIDs in the file, sampling stops early with a warning after extra attempts.
- **Table DDL / replicas**: create or alter the table to match `` `key_prefix_demo/main` `` (or change `mainDemoTable` in `main.go`). Apply `alter-main-read-replicas.sql` only if you intend to use read replicas; settings generally cannot be cleared once set.
