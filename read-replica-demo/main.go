// Command read-replica-demo demonstrates YDB read replicas with Stale Read Only isolation.
//
// Modes:
//
//	dump-keys  — paged SELECT via query service over primary key, writes UUIDs as 16-byte records (binary)
//	test-reads — samples KEYSET keys via one sequential pass (random skip/wrap), batched point reads
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
	"math"
	"math/rand"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// YQL table name relative to the database (see also fullTablePath for DescribeTable).
const mainDemoTable = "key_prefix_demo/main"

const progressLogInterval = 10 * time.Second

// startProgressLogger logs logFn every progressLogInterval until stop is called.
func startProgressLogger(logFn func()) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(progressLogInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				logFn()
			case <-done:
				return
			}
		}
	}()
	return func() {
		once.Do(func() { close(done) })
	}
}

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	ydbDSN := fs.String("ydb", "", "YDB connection string (required)")

	switch mode {
	case "dump-keys":
		outPath := fs.String("out", "keys.bin", "Output file: UUIDs as consecutive 16-byte records (RFC layout), YDB primary-key order")
		pageSize := fs.Uint64("page-size", 1000, "Rows per page")
		_ = fs.Parse(os.Args[2:])
		if *ydbDSN == "" {
			log.Fatal("-ydb is required")
		}
		if err := runDumpKeys(context.Background(), *ydbDSN, *outPath, *pageSize); err != nil {
			log.Fatal(err)
		}
	case "test-reads":
		keyFile := fs.String("keys", "keys.bin", "Key file from dump-keys (16-byte UUID records, no delimiters)")
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
		if err := runTestReads(context.Background(), *ydbDSN, *keyFile, *keyset, *batch, *rounds, *seed); err != nil {
			log.Fatal(err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  %s dump-keys  -ydb <dsn> [-out file] [-page-size N]
  %s test-reads -ydb <dsn> [-keys file] [-keyset N] [-batch N] [-rounds N] [-seed N]

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

func runDumpKeys(ctx context.Context, dsn, outPath string, pageSize uint64) error {
	db, err := openDB(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() { _ = db.Close(closeCtx); closeCancel() }()

	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer func() { _ = f.Close() }()
	w := bufio.NewWriter(f)
	row := make([]byte, 16)

	var last uuid.UUID
	var written atomic.Int64
	stopProgress := startProgressLogger(func() {
		log.Printf("dump-keys: progress %d keys written", written.Load())
	})
	defer stopProgress()

	// ORDER BY id uses MiniKQL CompareValues<Uuid> (memcmp on the same 16 bytes as
	// datashard cells), not canonical UUID text order. Do not use CAST(id AS String):
	// that sorts the human-readable form (mkql_type_ops ValueToString), which differs.
	dumpPageQuery := `DECLARE $limit AS Uint64;
DECLARE $last AS Uuid;
SELECT id FROM ` + "`" + mainDemoTable + "`" + `
WHERE id > $last
ORDER BY id
LIMIT $limit;`

	for page := 0; ; page++ {
		var rowsThisPage int
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
			params := ydb.ParamsBuilder().
				Param("$limit").Uint64(pageSize).
				Param("$last").Uuid(last).
				Build()
			rs, err := s.QueryResultSet(ctx, dumpPageQuery,
				query.WithParameters(params),
				query.WithTxControl(query.SnapshotReadOnlyTxControl()),
			)
			if err != nil {
				return err
			}
			defer func() { _ = rs.Close(ctx) }()
			for {
				resRow, err := rs.NextRow(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				var id uuid.UUID
				if err := resRow.ScanNamed(query.Named("id", &id)); err != nil {
					return err
				}
				copy(row, id[:])
				if _, err := w.Write(row); err != nil {
					return err
				}
				last = id
				rowsThisPage++
				written.Add(1)
			}
			return nil
		}, query.WithIdempotent())
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

	log.Printf("dump-keys: wrote %d keys to %s", written.Load(), outPath)
	return nil
}

type partitionRouter struct {
	upperBounds []uuid.UUID // exclusive right boundary per shard (omit open-ended last shard); len == #partitions or #partitions-1
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
			if i != len(desc.KeyRanges)-1 {
				return nil, fmt.Errorf("key range %d: upper bound is nil (only the last range may be unbounded)", i)
			}
			continue
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
	if desc.Stats == nil {
		return nil, errors.New("DescribeTable returned no stats (use WithPartitionStats)")
	}
	nParts := len(desc.Stats.PartitionStats)
	nRanges := len(desc.KeyRanges)
	if nParts != nRanges {
		return nil, fmt.Errorf("partition stats count %d != key ranges %d (use WithPartitionStats)", nParts, nRanges)
	}
	lastOpen := desc.KeyRanges[nRanges-1].To == nil
	expectUppers := nParts
	if lastOpen {
		expectUppers--
	}
	if len(pr.upperBounds) != expectUppers {
		return nil, fmt.Errorf("parsed %d upper bounds, expected %d (last key range open=%v, partitions=%d)",
			len(pr.upperBounds), expectUppers, lastOpen, nParts)
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
	// Key ranges are [from, to), so we need the first upper bound strictly greater than key.
	// If key is >= all explicit uppers, it belongs to the open-ended last partition.
	idx := sort.Search(len(pr.upperBounds), func(i int) bool {
		return compareYDBUUIDOrder(pr.upperBounds[i], k) > 0
	})
	if idx >= len(pr.nodeIDs) {
		idx = len(pr.nodeIDs) - 1
	}
	return idx
}

func percentileFromSorted(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	// Nearest-rank percentile: ceil(p/100*N), converted to zero-based index.
	idx := int(math.Ceil(float64(p)*float64(len(sorted))/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func logBatchLatencyStats(latencies []time.Duration) {
	if len(latencies) == 0 {
		log.Printf("test-reads: batch latency stats: no successful partition-queries")
		return
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	avgMs := float64(sum) / float64(len(sorted)) / float64(time.Millisecond)
	minMs := float64(sorted[0]) / float64(time.Millisecond)
	maxMs := float64(sorted[len(sorted)-1]) / float64(time.Millisecond)
	p50Ms := float64(percentileFromSorted(sorted, 50)) / float64(time.Millisecond)
	p90Ms := float64(percentileFromSorted(sorted, 90)) / float64(time.Millisecond)
	p99Ms := float64(percentileFromSorted(sorted, 99)) / float64(time.Millisecond)

	log.Printf("test-reads: batch latency stats (ms): n=%d min=%.3f max=%.3f avg=%.3f p50=%.3f p90=%.3f p99=%.3f",
		len(sorted), minMs, maxMs, avgMs, p50Ms, p90Ms, p99Ms)
}

// nextUUIDRecordOffset returns pos advanced by skipRecords 16-byte records, wrapping at EOF.
func nextUUIDRecordOffset(pos int64, skipRecords int64, size int64) int64 {
	if size <= 0 {
		return 0
	}
	p := pos + skipRecords*16
	p %= size
	return p
}

// sampleUniqueKeysFromFile reads fixed 16-byte UUID records (RFC byte order). It walks the file
// sequentially: each sample skips a random number of records (mean about nRecords/want, capped),
// reads one UUID, advances; at EOF position wraps to the start. Does not load the whole file into memory.
func sampleUniqueKeysFromFile(path string, want int, rng *rand.Rand) ([]uuid.UUID, int64, error) {
	if want <= 0 {
		return nil, 0, errors.New("want must be positive (want==0 is invalid)")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	if size == 0 {
		return nil, 0, errors.New("key file is empty")
	}
	if size%16 != 0 {
		return nil, size, fmt.Errorf("key file size %d is not a multiple of 16 bytes", size)
	}
	nRecords := size / 16
	if nRecords == 0 {
		return nil, size, errors.New("key file has no 16-byte UUID records (nRecords==0)")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()

	want64 := int64(want)
	if want64 > nRecords {
		log.Printf("test-reads: warning: want %d exceeds file record count %d; cannot collect that many distinct keys", want, nRecords)
	}

	const maxAttemptsMul = 64
	maxAttempts := want * maxAttemptsMul
	if maxAttempts < 8192 {
		maxAttempts = 8192
	}
	if want64 > nRecords {
		maxAttempts *= 4
	}

	out := make([]uuid.UUID, 0, want)
	seen := make(map[uuid.UUID]struct{}, want)
	buf := make([]byte, 16)
	var pos int64

	var keysProg, attemptsProg atomic.Int64
	stopSampleProgress := startProgressLogger(func() {
		log.Printf("test-reads: key sample progress %d / %d unique keys (attempts %d)",
			keysProg.Load(), want64, attemptsProg.Load())
	})
	defer stopSampleProgress()

	// Target mean skip ≈ nRecords/want: uniform on [0, min(2*avgSkip, nRecords-1)] when avgSkip>0; else skip=0.
	avgSkip := nRecords / want64

	for attempt := 0; len(out) < want && attempt < maxAttempts; attempt++ {
		attemptsProg.Store(int64(attempt + 1))
		var skip int64
		if avgSkip == 0 {
			skip = 0
		} else {
			span := 2 * avgSkip
			if span > nRecords-1 {
				span = nRecords - 1
			}
			skip = rng.Int63n(span + 1)
		}
		pos = nextUUIDRecordOffset(pos, skip, size)
		if _, err := f.Seek(pos, io.SeekStart); err != nil {
			return nil, size, err
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, size, err
		}
		id, err := uuid.FromBytes(buf)
		if err != nil {
			return nil, size, fmt.Errorf("invalid 16-byte UUID at offset %d: %w", pos, err)
		}
		pos = nextUUIDRecordOffset(pos, 1, size)
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
		keysProg.Store(int64(len(out)))
	}

	if len(out) == 0 {
		return nil, size, errors.New("no keys sampled from file")
	}
	if len(out) < want {
		log.Printf("test-reads: warning: only sampled %d unique keys (wanted %d); small keyspace or many duplicates", len(out), want)
	}
	return out, size, nil
}

func runTestReads(ctx context.Context, dsn, keyFile string, keysetSize, batchSize, rounds int, seed int64) error {
	db, err := openDB(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close(ctx) }()

	tableFull := fullTablePath(db, mainDemoTable)

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

	var okBatches, rowCount atomic.Int64
	var roundDone atomic.Int64
	var latMu sync.Mutex
	latencies := make([]time.Duration, 0, rounds)
	defer func() {
		latMu.Lock()
		snapshot := append([]time.Duration(nil), latencies...)
		latMu.Unlock()
		logBatchLatencyStats(snapshot)
	}()
	rounds64 := int64(rounds)
	stopExecProgress := startProgressLogger(func() {
		log.Printf("test-reads: execution progress rounds %d / %d, partition-queries %d, rows %d",
			roundDone.Load(), rounds64, okBatches.Load(), rowCount.Load())
	})
	defer stopExecProgress()

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

				items := make([]types.Value, len(ks))
				for i, id := range ks {
					items[i] = types.UuidValue(id)
				}
				params := ydb.ParamsBuilder().Param("$ids").BeginList().AddItems(items...).EndList().Build()

				q := `DECLARE $ids AS List<Uuid>;
SELECT id, collection_id, tv, ballast1 FROM ` + "`" + mainDemoTable + "`" + ` WHERE id IN $ids;`

				qctx, qcancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer qcancel()

				start := time.Now()
				err := db.Query().Do(qctx, func(fctx context.Context, s query.Session) error {
					rs, err := s.QueryResultSet(fctx, q,
						query.WithParameters(params),
						query.WithTxControl(query.StaleReadOnlyTxControl()),
					)
					if err != nil {
						return err
					}
					defer func() { _ = rs.Close(fctx) }()
					var rows int
					for {
						row, err := rs.NextRow(fctx)
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							return err
						}
						var id string
						if err := row.ScanNamed(query.Named("ballast1", &id)); err != nil {
							return err
						}
						rows++
					}
					okBatches.Add(1)
					rowCount.Add(int64(rows))
					return nil
				}, query.WithIdempotent())
				if err != nil {
					errCh <- fmt.Errorf("partition %d (%d keys): %w", pi, len(ks), err)
				}
				latMu.Lock()
				latencies = append(latencies, time.Since(start))
				latMu.Unlock()
			}(pi, ks)
		}
		innerWg.Wait()
		close(errCh)
		for e := range errCh {
			return e
		}
		roundDone.Store(int64(r + 1))
	}

	log.Printf("test-reads: completed %d outer rounds, %d partition-queries, %d rows returned",
		rounds, okBatches.Load(), rowCount.Load())
	return nil
}
