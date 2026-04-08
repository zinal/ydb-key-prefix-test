# YDB Key Prefix Demo and Test

## YDB-friendly UUIDs

This project demonstrates a **structured random UUID** (`UuidKeyGen`) instead of a plain RFC 4122 version-4 UUID where all random bits are independent. The 128-bit value is still a valid UUID (version and variant bits are set correctly), but the **most significant bits are arranged on purpose** so that key order and partitioning behavior suit YDB's row-organized tables.

### How it works

The generator fills 16 random bytes, then **rewrites part of the MSB** so that three logical fields share the top of the 64-bit word:

1. **Prefix (1–18 bits, default 10)** — A small set of high bits. Normally each ID draws a **new random prefix**, so the workload is **spread across on the order of 2^prefixBits partition ranges** (for example about a thousand buckets with the default 10 bits). That **horizontal spread is important for scalability**: data and load are not funneled through a single partition. At the same time, the prefix is **short relative to the full 128-bit key**, so within each prefix bucket **rows whose embedded timestamps are close** still sit in **tight subranges** — providing good **data locality and cache-friendly** access when you touch recent or time-ordered data inside a shard.
2. **Timestamp code (30 bits)** — Second-granularity time is encoded in a fixed bit range (the exact position shifts when you change the prefix width). The code uses **Unix epoch seconds reduced modulo 2³⁰** (a window of about 34 years before the pattern repeats).
3. **Remaining MSB bits** — Still random, so IDs stay unique.

The **least significant 64 bits** stay fully random. Within a **fixed** prefix, UUID order follows **time then uniqueness**, so similar times group together; **varying** the prefix between IDs restores wide partition spread for the default API.

For **single-transaction, multi-row writes**, there is a custom API which allows to generate a fixed prefix value and then use it for each row. That **pins one prefix** for the whole batch, so those keys **typically map to a single partition**, which **reduces cross-partition work and transaction complexity** for that operation compared to giving every row an independent random prefix.

### Why this helps YDB performance

YDB partitions tables by **primary key ranges**. With **fully random UUIDv4**, each new key is uniformly scattered over the entire range. That maximizes partition spread for writes and can make related rows land in unrelated ranges, which hurts locality and can stress automatic splitting and cross-partition work.

A **YDB-friendly** layout does the opposite of “pure chaos” in a controlled way:

- **Time in the key** — Keys created close together in time tend to sit in **adjacent key ranges**, so inserts and time-bounded scans touch **fewer partitions** than completely random keys, and auto-partitioning can follow natural workload boundaries more smoothly.
- **Shared prefix for one transaction** — When you fix one prefix for every row in a batch, those rows **usually land in one partition**, which simplifies that transaction. The default path **keeps random prefixes** so the table stays **well split** for overall throughput.
- **Secondary indexes** — Global indexes are also range-partitioned on their index key. If indexed columns use the same kind of structured UUID, index maintenance and lookups benefit from the same locality properties.

### Application usage

**Typical usage** is the zero-argument path: call **`nextValue()`** with no parameters. The implementation picks a **random prefix** and embeds the **current time** (second precision) in the MSB (most significant bits part) alongside more randomness. It provides the same “drop-in” ergonomics as a normal UUID factory, while still gaining time-aware key locality compared to fully unstructured random IDs.

For a **custom transactional pattern**, the API supports generating a sequence of values **sharing one prefix**: call **`nextPrefix()`** once, then **`nextValue(prefix, …)`** for every row in the same batch or transaction. Those keys **typically fall into a single partition**, which **lowers the cost and complexity of that one transaction** (fewer shards involved) compared to calling **`nextValue()`** per row, where each new random prefix preserves **broad partition spread** for scalability.

**Backdated or historical data** is covered by overloads that take an **`Instant`** or **`LocalDate`** instead of using the clock inside the generator—for example **`nextValue(instant)`** with a random prefix per call, or **`nextValue(prefix, instant)`** / **`nextValue(prefix, date)`** when you both group by prefix and pin the embedded time to a past period.

## Application example that demonstrates the behavior

The runnable demo is class **`tech.ydb.samples.keyprefix.Main`**. It reads an **XML properties** file (same format as Java `Properties` stored as XML), connects with **YDB JDBC**, and runs one of five **execution modes**. Build the project, point the config at your database, then run the jar with **two arguments**: the config path and the mode name.

```bash
mvn clean package -DskipTests=true
java -jar target/ydb-key-prefix-demo-1.0-SNAPSHOT.jar /path/to/config.xml INIT
```

Use a **working directory** where relative paths from the config resolve correctly: `ddl.file` and `gen.ballast.file` are read with `java.nio.file.Path` as given (relative paths are relative to the process current working directory).

### Execution modes

| Mode | What it does |
|------|----------------|
| **`INIT`** | Executes every statement in the DDL file (`ddl.file`), split on semicolons. Creates `key_prefix_demo/main` and `key_prefix_demo/sub` and their indexes (see your SQL script for exact definitions). |
| **`FILL`** | Loads synthetic data for each calendar day from **`gen.start`** through **`gen.finish`** (inclusive). Days are processed **concurrently** up to **`gen.threads`** workers. For each day, runs **`gen.scale`** generator steps; each step inserts batches of rows into `main` and `sub` with shared-prefix UUIDs when **`gen.uuid.v8`** is enabled (see below). |
| **`TEST`** | Read-heavy stress: **`test.threads`** workers each run **`test.iterations`** loops. Each loop picks a random time on **`test.day`** (in **`Europe/Moscow`**, hardcoded in `Main`) and runs two queries that scan global indexes on **`tv`**, **`LIMIT`** **`test.rows`**, and join through **`collection_id` / `ref_id`**. Use after **`INIT`** and **`FILL`**. |
| **`CLEAN`** | Drops **`key_prefix_demo/sub`** then **`key_prefix_demo/main`**. |
| **`PRINT`** | Prints **`TextKeyGen`** IDs to stdout in an **infinite loop** (handy for quick inspection; stop with Ctrl+C). Does not use the database. |

Recommended order for a full demo on a test database: **`INIT`** → **`FILL`** → **`TEST`** → **`CLEAN`** when finished.

### Configuration parameters

All entries are optional unless noted; omitted keys fall back to **Java defaults** in `Main.Config` (see source). The sample file is `scripts/sample-config.xml`.

| Property | Purpose |
|----------|---------|
| **`ydb.url`** | JDBC URL for YDB (required for all modes except **`PRINT`**). Example: `jdbc:ydb:grpcs://host:2135/Root/testdb?...` |
| **`ydb.user`** | Database user. |
| **`ydb.password`** | Database password. |
| **`ddl.file`** | Path to the DDL script executed by **`INIT`** (required for **`INIT`**). |
| **`gen.ballast.file`** | Text file of lines used to build ~500-character **`ballast1` / `ballast2`** strings on **`FILL`**. Required for realistic **`FILL`** unless you change the code. |
| **`gen.uuid.v8`** | Boolean, default **`true`**. When **`true`**, **`FILL`** uses **`UuidKeyGen`** (structured keys). When **`false`**, **`FILL`** uses **`UUID.randomUUID()`** (plain UUIDv4) so you can compare behavior under the same load shape. |
| **`gen.scale`** | Integer, default **`1`**. “Scale units” per calendar day on **`FILL`** (each unit issues one transactional batch pattern in the generator). Comment in the sample config describes it as **thousands of records per day per table**—treat the name as historical; confirm volume against `Main.fillDate` / `fillDateStep` if you need exact row counts. |
| **`gen.start`** | First calendar date for **`FILL`** (`YYYY-MM-DD`). |
| **`gen.finish`** | Last calendar date for **`FILL`** (`YYYY-MM-DD`). |
| **`gen.threads`** | Size of the thread pool for **`FILL`** (default **`4`** if unset). |
| **`test.threads`** | Concurrent workers for **`TEST`** (default **`4`**). |
| **`test.day`** | Calendar date used as the base day for random query timestamps on **`TEST`**. |
| **`test.iterations`** | Number of read iterations per **`TEST`** worker (default **`100`**). |
| **`test.rows`** | **`LIMIT`** for each index-driven subquery inside **`TEST`** (default **`10`**). |
| **`retry.count`** | Extra attempts on **`YdbRetryableException`** / **`YdbConditionallyRetryableException`** for **`FILL`** and **`TEST`** (default **`10`**). |

The JDBC pool size is set to **twice** the larger of **`gen.threads`** and **`test.threads`**.

### What **`TEST`** is measuring

**`TEST`** is a **live integration** workload: latency and throughput depend on cluster size, data volume, and partitioning. It exercises the same access path as typical time-range + index + join traffic on the demo schema, so you can contrast runs with **`gen.uuid.v8`** **`true`** vs **`false`** after reloading data.

## Building

Maven, Java SDK 21

```bash
mvn clean package -DskipTests=true
```
