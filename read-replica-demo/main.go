// Command read-replica-demo demonstrates YDB read replicas with Stale Read Only isolation.
//
// Modes:
//
//	dump-keys  — paged SELECT over primary key, writes sorted UUIDs to a file
//	test-reads — samples KEYSET keys from a key file (random seeks; no full load), runs batched point reads
//	             split by table partitions with preferred node hints, concurrently
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	ydbDSN := fs.String("ydb", "", "YDB connection string (required)")
	tablePath := fs.String("table", "key_prefix_demo/main", "Table path relative to database root")

	switch mode {
	case "dump-keys":
		outPath := fs.String("out", "keys_sorted.txt", "Output file: one canonical UUID string per line, in YDB primary-key (Uuid cell) order")
		pageSize := fs.Uint64("page-size", 1000, "Rows per page")
		_ = fs.Parse(os.Args[2:])
		if *ydbDSN == "" {
			log.Fatal("-ydb is required")
		}
		if err := runDumpKeys(context.Background(), *ydbDSN, *tablePath, *outPath, *pageSize); err != nil {
			log.Fatal(err)
		}
	case "test-reads":
		keyFile := fs.String("keys", "keys_sorted.txt", "Key file from dump-keys (YDB Uuid cell order, one UUID per line)")
		keyset := fs.Int("keyset", 10000, "Number of unique random keys to sample from the file")
		batch := fs.Int("batch", 32, "Max keys per SELECT batch")
		rounds := fs.Int("rounds", 100, "How many batches to run (each picks BATCH keys at random from the sampled keyset)")
		seed := fs.Int64("seed", time.Now().UnixNano(), "RNG seed for sampling and batch composition")
		_ = fs.Parse(os.Args[2:])
		if *ydbDSN == "" {
			log.Fatal("-ydb is required")
		}
		if *keyset <= 0 || *batch <= 0 || *rounds <= 0 {
			log.Fatal("-keyset, -batch, and -rounds must be positive")
		}
		if err := runTestReads(context.Background(), *ydbDSN, *tablePath, *keyFile, *keyset, *batch, *rounds, *seed); err != nil {
			log.Fatal(err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  %s dump-keys  -ydb <dsn> [-table path] [-out file] [-page-size N]
  %s test-reads -ydb <dsn> [-table path] [-keys file] [-keyset N] [-batch N] [-rounds N] [-seed N]

Environment credentials follow ydb-go-sdk-auth-environ (e.g. YDB_ANONYMOUS_CREDENTIALS=1).
If both YDB_USER and YDB_PASSWORD are set, static credentials are used instead.

For custom TLS certificates, set YDB_SSL_ROOT_CERTIFICATES_FILE.

`, os.Args[0], os.Args[0])
}

func openDB(ctx context.Context, dsn string) (*ydb.Driver, error) {
	user := os.Getenv("YDB_USER")
	password := os.Getenv("YDB_PASSWORD")
	if user != "" || password != "" {
		if user == "" || password == "" {
			return nil, errors.New("static auth requires both YDB_USER and YDB_PASSWORD (or unset both to use ydb-go-sdk-auth-environ)")
		}
		return ydb.Open(ctx, dsn, ydb.WithStaticCredentials(user, password))
	}
	return ydb.Open(ctx, dsn, environ.WithEnvironCredentials())
}

func fullTablePath(db *ydb.Driver, relative string) string {
	return path.Join(db.Name(), strings.TrimPrefix(relative, "/"))
}

// uuidYDBCellBytes returns the 16-byte representation YDB uses for UUID keys and YQL Uuid
// comparisons: lexicographic order on this slice matches table storage (memcmp on cells),
// YQL ORDER BY id, and NUuid::ParseUuidToArray in ydb-platform/ydb
// (see ydb/core/scheme/scheme_tablecell.h CompareTypedCells Uuid,
// yql/essentials/public/udf/udf_type_ops.h CompareValues<EDataSlot::Uuid>).
// It matches github.com/ydb-platform/ydb-go-sdk/v3/internal/value.uuidDirectBytesToLe.
func uuidYDBCellBytes(id uuid.UUID) [16]byte {
	b := id
	var le [16]byte
	le[0], le[1], le[2], le[3] = b[3], b[2], b[1], b[0]
	le[4], le[5], le[6], le[7] = b[5], b[4], b[7], b[6]
	copy(le[8:], b[8:])
	return le
}

func compareYDBUUIDOrder(a, b uuid.UUID) int {
	ka := uuidYDBCellBytes(a)
	kb := uuidYDBCellBytes(b)
	return slices.Compare(ka[:], kb[:])
}

func runDumpKeys(ctx context.Context, dsn, tableRel, outPath string, pageSize uint64) error {
	db, err := openDB(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close(ctx) }()

	tableFull := fullTablePath(db, tableRel)
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer func() { _ = f.Close() }()
	w := bufio.NewWriter(f)

	var last uuid.UUID
	var n int64
	readTx := table.TxControl(table.BeginTx(table.WithSnapshotReadOnly()), table.CommitTx())

	for page := 0; ; page++ {
		var rowsThisPage int
		err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			// ORDER BY id uses MiniKQL CompareValues<Uuid> (memcmp on the same 16 bytes as
			// datashard cells), not canonical UUID text order. Do not use CAST(id AS String):
			// that sorts the human-readable form (mkql_type_ops ValueToString), which differs.
			q := fmt.Sprintf(`
				DECLARE $limit AS Uint64;
				DECLARE $last AS Uuid;
				SELECT id FROM %s
				WHERE id > $last
				ORDER BY id
				LIMIT $limit;
			`, "`"+tableFull+"`")

			_, res, err := s.Execute(ctx, readTx, q, table.NewQueryParameters(
				table.ValueParam("$limit", types.Uint64Value(pageSize)),
				table.ValueParam("$last", types.UuidValue(last)),
			))
			if err != nil {
				return err
			}
			defer func() { _ = res.Close() }()

			if !res.NextResultSet(ctx) {
				return errors.New("no result set")
			}
			for res.NextRow() {
				var id uuid.UUID
				if err := res.ScanNamed(named.Required("id", &id)); err != nil {
					return err
				}
				if _, err := fmt.Fprintln(w, id.String()); err != nil {
					return err
				}
				last = id
				rowsThisPage++
				n++
			}
			return res.Err()
		}, table.WithIdempotent())
		if err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if rowsThisPage == 0 {
			break
		}
	}

	log.Printf("dump-keys: wrote %d keys to %s", n, outPath)
	return nil
}

type partitionRouter struct {
	upperBounds []uuid.UUID // exclusive right boundary per shard; len == #partitions
	nodeIDs     []uint32    // LeaderNodeID per partition (for preferred-node hint)
}

func buildPartitionRouter(desc options.Description) (*partitionRouter, error) {
	if len(desc.KeyRanges) == 0 {
		return nil, errors.New("DescribeTable returned no key ranges (try WithShardKeyBounds)")
	}
	pr := &partitionRouter{
		upperBounds: make([]uuid.UUID, 0, len(desc.KeyRanges)),
		nodeIDs:     make([]uint32, 0, len(desc.KeyRanges)),
	}
	for i, kr := range desc.KeyRanges {
		if kr.To == nil {
			return nil, fmt.Errorf("key range %d: upper bound is nil (unexpected)", i)
		}
		parts, err := types.TupleItems(kr.To)
		if err != nil || len(parts) == 0 {
			return nil, fmt.Errorf("key range %d: tuple: %w", i, err)
		}
		var upper uuid.UUID
		inner := types.Unwrap(parts[0])
		if err := types.CastTo(inner, &upper); err != nil {
			return nil, fmt.Errorf("key range %d: cast upper to uuid: %w", i, err)
		}
		pr.upperBounds = append(pr.upperBounds, upper)
	}
	if desc.Stats == nil || len(desc.Stats.PartitionStats) != len(pr.upperBounds) {
		return nil, fmt.Errorf("partition stats count %d != key ranges %d (use WithPartitionStats)",
			len(desc.Stats.PartitionStats), len(pr.upperBounds))
	}
	for _, ps := range desc.Stats.PartitionStats {
		pr.nodeIDs = append(pr.nodeIDs, ps.LeaderNodeID)
	}
	for i := 1; i < len(pr.upperBounds); i++ {
		if compareYDBUUIDOrder(pr.upperBounds[i-1], pr.upperBounds[i]) >= 0 {
			return nil, fmt.Errorf("shard upper bounds not strictly increasing at %d", i)
		}
	}
	return pr, nil
}

func (pr *partitionRouter) partitionIndexForKey(k uuid.UUID) int {
	idx, found := slices.BinarySearchFunc(pr.upperBounds, k, func(upper uuid.UUID, key uuid.UUID) int {
		return compareYDBUUIDOrder(key, upper)
	})
	if found {
		idx++
	}
	if idx < 0 {
		idx = 0
	}
	if idx >= len(pr.nodeIDs) {
		idx = len(pr.nodeIDs) - 1
	}
	return idx
}

func (pr *partitionRouter) withPreferredNode(ctx context.Context, k uuid.UUID) context.Context {
	idx := pr.partitionIndexForKey(k)
	return ydb.WithPreferredNodeID(ctx, pr.nodeIDs[idx])
}

// errSkipUUIDSample means the random seek did not yield a usable line; try again.
var errSkipUUIDSample = errors.New("skip uuid sample")

// readUUIDLineAtRandomOffset seeks to a random byte in [0, size), skips the partial line when not
// at offset 0, then returns the next complete line as a UUID. Suitable for huge files without
// scanning them end-to-end (lines should be similar length for roughly uniform line sampling).
func readUUIDLineAtRandomOffset(f *os.File, size int64, rng *rand.Rand) (uuid.UUID, error) {
	if size <= 0 {
		return uuid.UUID{}, errSkipUUIDSample
	}
	off := rng.Int63n(size)
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return uuid.UUID{}, err
	}
	r := bufio.NewReader(f)
	if off > 0 {
		if _, err := r.ReadBytes('\n'); err != nil {
			if errors.Is(err, io.EOF) {
				return uuid.UUID{}, errSkipUUIDSample
			}
			return uuid.UUID{}, err
		}
	}
	line, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return uuid.UUID{}, err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return uuid.UUID{}, errSkipUUIDSample
	}
	id, err := uuid.Parse(line)
	if err != nil {
		return uuid.UUID{}, errSkipUUIDSample
	}
	return id, nil
}

// sampleUniqueKeysFromFile picks want distinct keys by random byte offsets into the file (see
// readUUIDLineAtRandomOffset). It does not read or buffer the whole file.
func sampleUniqueKeysFromFile(path string, want int, rng *rand.Rand) ([]uuid.UUID, int64, error) {
	if want <= 0 {
		return nil, 0, errors.New("want must be positive")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	if size == 0 {
		return nil, 0, errors.New("key file is empty")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()

	const maxAttemptsMul = 64
	maxAttempts := want * maxAttemptsMul
	if maxAttempts < 8192 {
		maxAttempts = 8192
	}

	out := make([]uuid.UUID, 0, want)
	seen := make(map[uuid.UUID]struct{}, want)

	for attempt := 0; len(out) < want && attempt < maxAttempts; attempt++ {
		id, err := readUUIDLineAtRandomOffset(f, size, rng)
		if errors.Is(err, errSkipUUIDSample) {
			continue
		}
		if err != nil {
			return nil, size, err
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}

	if len(out) == 0 {
		return nil, size, errors.New("no valid UUID lines sampled (empty or invalid file content?)")
	}
	if len(out) < want {
		log.Printf("test-reads: warning: only sampled %d unique keys (wanted %d); small file, short lines, or many duplicates", len(out), want)
	}
	return out, size, nil
}

func runTestReads(ctx context.Context, dsn, tableRel, keyFile string, keysetSize, batchSize, rounds int, seed int64) error {
	db, err := openDB(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close(ctx) }()

	tableFull := fullTablePath(db, tableRel)

	var pr *partitionRouter
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		desc, err := s.DescribeTable(ctx, tableFull,
			options.WithTableStats(),
			options.WithPartitionStats(),
			options.WithShardKeyBounds(),
			options.WithShardNodesInfo(),
		)
		if err != nil {
			return err
		}
		pr, err = buildPartitionRouter(desc)
		return err
	}, table.WithIdempotent())
	if err != nil {
		return fmt.Errorf("describe table: %w", err)
	}

	rng := rand.New(rand.NewSource(seed))
	keyset, fileSize, err := sampleUniqueKeysFromFile(keyFile, keysetSize, rng)
	if err != nil {
		return fmt.Errorf("sample keys: %w", err)
	}

	log.Printf("test-reads: key file %d bytes, keyset %d unique keys, %d partitions",
		fileSize, len(keyset), len(pr.nodeIDs))

	var okBatches, rowCount int64
	var mu sync.Mutex

	for r := 0; r < rounds; r++ {
		if len(keyset) == 0 {
			break
		}
		bn := batchSize
		if bn > len(keyset) {
			bn = len(keyset)
		}
		batch := make([]uuid.UUID, bn)
		for i := range batch {
			batch[i] = keyset[rng.Intn(len(keyset))]
		}
		slices.SortFunc(batch, compareYDBUUIDOrder)
		batch = slices.Compact(batch)
		if len(batch) == 0 {
			continue
		}

		byPart := make([][]uuid.UUID, len(pr.nodeIDs))
		for _, k := range batch {
			pi := pr.partitionIndexForKey(k)
			byPart[pi] = append(byPart[pi], k)
		}

		var innerWg sync.WaitGroup
		errCh := make(chan error, len(byPart))
		for pi, ks := range byPart {
			if len(ks) == 0 {
				continue
			}
			innerWg.Add(1)
			go func(pi int, ks []uuid.UUID) {
				defer innerWg.Done()
				hintKey := ks[rng.Intn(len(ks))]
				partCtx := pr.withPreferredNode(ctx, hintKey)

				items := make([]types.Value, len(ks))
				for i, id := range ks {
					items[i] = types.StructValue(
						types.StructFieldValue("id", types.UuidValue(id)),
					)
				}
				params := ydb.ParamsBuilder().Param("$ids").BeginList().AddItems(items...).EndList().Build()

				q := fmt.Sprintf(`
					DECLARE $ids AS List<Struct<id: Uuid>>;
					SELECT m.id AS id
					FROM %s AS m
					INNER JOIN AS_TABLE($ids) AS k ON m.id = k.id;
				`, "`"+tableFull+"`")

				err := db.Query().DoTx(partCtx, func(ctx context.Context, tx query.TxActor) error {
					rs, err := tx.QueryResultSet(ctx, q, query.WithParameters(params))
					if err != nil {
						return err
					}
					defer func() { _ = rs.Close(ctx) }()
					var rows int
					for {
						row, err := rs.NextRow(ctx)
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							return err
						}
						var id uuid.UUID
						if err := row.ScanNamed(query.Named("id", &id)); err != nil {
							return err
						}
						rows++
					}
					mu.Lock()
					okBatches++
					rowCount += int64(rows)
					mu.Unlock()
					return nil
				}, query.WithIdempotent(), query.WithTxSettings(query.TxSettings(query.WithStaleReadOnly())))
				if err != nil {
					errCh <- fmt.Errorf("partition %d (%d keys): %w", pi, len(ks), err)
				}
			}(pi, ks)
		}
		innerWg.Wait()
		close(errCh)
		for e := range errCh {
			return e
		}
	}

	log.Printf("test-reads: completed %d outer rounds, %d partition-queries, %d rows returned", rounds, okBatches, rowCount)
	return nil
}
