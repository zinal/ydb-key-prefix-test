package tech.ydb.samples.keyprefix;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import tech.ydb.jdbc.exception.YdbConditionallyRetryableException;
import tech.ydb.jdbc.exception.YdbRetryableException;

/**
 *
 * @author zinal
 */
public class Main implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private final Config config;
    private final HikariDataSource ds;
    private final UuidKeyGen keyGen;
    private final ArrayList<String> ballastLines;
    private final ZoneId timeZone;
    private final AtomicInteger tasksRunning = new AtomicInteger();
    private final AtomicLong itemsCompleted = new AtomicLong();
    private final AtomicLong itemsExpected = new AtomicLong();
    private final AtomicLong rowsCompleted = new AtomicLong();

    public Main(Config sc) {
        this.config = sc;
        this.ds = createDataSource(sc);
        this.keyGen = new UuidKeyGen();
        this.ballastLines = readBallastLines(sc.getBallastFile());
        this.timeZone = ZoneId.of("Europe/Moscow");
    }

    @Override
    public void close() {
        ds.close();
    }

    public void actionInit() throws Exception {
        LOG.info("Init started...");
        runDdlScript();
        LOG.info("Init successfull!");
    }

    public void actionClean() throws Exception {
        LOG.info("Cleanup started...");
        dropTables();
        LOG.info("Cleanup successfull!");
    }

    public void actionFill() throws Exception {
        try (var service = Executors.newFixedThreadPool(config.getGeneratorThreads())) {
            LOG.info("Submitting fill tasks with UUIDv8={} ...", config.isUuidV8());
            var tasks = new ArrayList<Future<?>>();
            itemsCompleted.set(0L);
            rowsCompleted.set(0L);
            // one task per date
            LocalDate current = config.getGeneratorStart();
            while (!current.isAfter(config.getGeneratorFinish())) {
                LocalDate dt = current;
                var task = service.submit(() -> fillDate(dt));
                tasks.add(task);
                current = current.plusDays(1);
            }
            itemsExpected.set(1L * tasks.size()
                    * 1L * config.getGeneratorScale());
            LOG.info("Fill started...");
            waitForCompletion(tasks);
            LOG.info("Fill successful!");
        }
    }

    public void actionTest() throws Exception {
        try (var service = Executors.newFixedThreadPool(config.getTestThreads())) {
            LOG.info("Submitting test tasks...");
            var tasks = new ArrayList<Future<?>>();
            itemsCompleted.set(0L);
            rowsCompleted.set(0L);
            itemsExpected.set(1L * config.getTestThreads()
                    * 1L * config.getTestIterations());
            var startedAt = Instant.now();
            LocalDate testDay = config.getTestDay();
            for (int i = 0; i < config.getTestThreads(); ++i) {
                var task = service.submit(() -> testTask(testDay));
                tasks.add(task);
            }
            LOG.info("Test started...");
            waitForCompletion(tasks);
            long elapsedSeconds = startedAt.until(Instant.now(), ChronoUnit.SECONDS);
            LOG.info("Test successful, total {} iterations in {} seconds!",
                    itemsCompleted.get(), elapsedSeconds);
        }
    }

    public void actionPrint() {
        for (int i = 0; i < 100; ++i) {
            System.out.println(newId(keyGen.nextPrefix(), Instant.now()));
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            LOG.info("Two arguments are expected: config-file.xml { INIT | FILL | TEST | CLEAN | PRINT | LAYOUT | ORDER }");
            System.exit(2);
        }
        try {
            var action = Action.valueOf(args[1]);
            LOG.info("Reading configuration {}...", args[0]);
            var config = readConfig(args[0]);
            try (var m = new Main(config)) {
                LOG.info("Initialized, executing {}.", action);
                switch (action) {
                    case INIT -> {
                        m.actionInit();
                    }
                    case FILL -> {
                        m.actionFill();
                    }
                    case TEST -> {
                        m.actionTest();
                    }
                    case CLEAN -> {
                        m.actionClean();
                    }
                    case PRINT -> {
                        m.actionPrint();
                    }
                    case LAYOUT -> {
                        m.actionLayout();
                    }
                    case ORDER -> {
                        m.actionOrder();
                    }
                }
            }
            LOG.info("Completed, shutting down.");
        } catch (Exception ex) {
            LOG.error("FATAL", ex);
            System.exit(1);
        }
    }

    public static HikariDataSource createDataSource(Config sc) {
        var maxConnections = 2 * Math.max(sc.getGeneratorThreads(), sc.getTestThreads());
        LOG.info("Configuring JDBC data source for {}, maxConnections {}",
                sc.getUrl(), maxConnections);
        HikariConfig hc = new HikariConfig();
        hc.setAutoCommit(false);
        hc.setJdbcUrl(sc.getUrl());
        hc.setUsername(sc.getLogin());
        hc.setPassword(sc.getPassword());
        hc.setMaximumPoolSize(maxConnections);
        return new HikariDataSource(hc);
    }

    private void runDdlScript() throws Exception {
        String regex = ";\\s*(?=([^']*'[^']*')*[^']*$)";
        var sqls = Files.readString(
                Path.of(config.ddlFile), StandardCharsets.UTF_8)
                .split(regex);
        try (var conn = ds.getConnection()) {
            conn.setAutoCommit(true);
            try (var stmt = conn.createStatement()) {
                for (var sql : sqls) {
                    stmt.execute(sql);
                }
            } finally {
                conn.setAutoCommit(false);
            }
        }
    }

    private void dropTables() throws Exception {
        try (var conn = ds.getConnection()) {
            conn.setAutoCommit(true);
            try (var stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE `key_prefix_demo/sub`");
                stmt.execute("DROP TABLE `key_prefix_demo/main`");
            } finally {
                conn.setAutoCommit(false);
            }
        }
    }

    private void testTask(LocalDate testDay) {
        tasksRunning.incrementAndGet();
        try {
            for (int iter = 0; iter < config.getTestIterations(); ++iter) {
                runWithRetry(true, (con) -> testTaskIter(con, testDay));
                itemsCompleted.incrementAndGet();
            }
        } finally {
            tasksRunning.decrementAndGet();
        }
    }

    private void testTaskIter(Connection con, LocalDate testDay) throws Exception {
        int rows = 0;
        long seconds = ThreadLocalRandom.current().nextLong(0L, 60L * 60L * 23L);
        var tv = testDay.atStartOfDay(timeZone).plus(seconds, ChronoUnit.SECONDS);
        Timestamp ts = Timestamp.from(tv.toInstant());
        String sql;

        sql = """
SELECT main.id, sub.id, main.collection_id, main.ballast1, sub.ballast2
FROM (SELECT id
      FROM `key_prefix_demo/main` VIEW ix_tv
      WHERE tv >= ?
      ORDER BY tv LIMIT ?) AS main_ids
INNER JOIN `key_prefix_demo/main` AS main
    ON main_ids.id = main.id
LEFT JOIN `key_prefix_demo/sub` VIEW ix_ref AS sub
    ON sub.ref_id = main.collection_id;
              """;
        try (var ps = con.prepareStatement(sql)) {
            ps.setTimestamp(1, ts);
            ps.setInt(2, config.getTestRows());
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    ++rows;
                }
            }
        }

        sql = """
SELECT main.id, sub.id, main.collection_id, main.ballast1, sub.ballast2
FROM (SELECT id
      FROM `key_prefix_demo/sub` VIEW ix_tv
      WHERE tv >= ?
      ORDER BY tv LIMIT ?) AS sub_ids
INNER JOIN `key_prefix_demo/sub` AS sub
    ON sub_ids.id = sub.id
LEFT JOIN `key_prefix_demo/main` VIEW ix_coll AS main
    ON sub.ref_id = main.collection_id;
              """;
        try (var ps = con.prepareStatement(sql)) {
            ps.setTimestamp(1, ts);
            ps.setInt(2, config.getTestRows());
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    ++rows;
                }
            }
        }

        rowsCompleted.addAndGet(rows);
    }

    private void fillDate(LocalDate dt) {
        LOG.debug("Filling data for {}...", dt);
        tasksRunning.incrementAndGet();
        try {
            for (int i = 0; i < config.getGeneratorScale(); ++i) {
                long prefix = newPrefix();
                var tv = newTv(dt);
                var entries = IntStream.range(0, 200)
                        .mapToObj(ix -> newDataEntry(ix, prefix, tv))
                        .toList();
                runWithRetry(false, (con) -> fillDateStep(con, entries));
                itemsCompleted.incrementAndGet();
                rowsCompleted.addAndGet(2 * entries.size());
            }
        } catch (Exception ex) {
            LOG.error("Failed to fill for {}", dt, ex);
            return;
        } finally {
            tasksRunning.decrementAndGet();
        }
        LOG.debug("Completed filling data for {}.", dt);
    }

    private void fillDateStep(Connection con, List<DataEntry> entries) throws Exception {
        // 5 iterations for 200 lines each
        for (int i = 0; i < 5; ++i) {
            String sql = """
    UPSERT INTO `key_prefix_demo/main`(id, collection_id, tv, ballast1)
    VALUES(?, ?, ?, ?);
    """;
            try (var ps = con.prepareStatement(sql)) {
                for (var entry : entries) {
                    ps.setObject(1, entry.mainId);
                    ps.setObject(2, entry.refId);
                    ps.setTimestamp(3, Timestamp.from(entry.tv));
                    ps.setString(4, entry.ballast1);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            sql = """
    UPSERT INTO `key_prefix_demo/sub`(id, ref_id, tv, ballast2)
    VALUES(?, ?, ?, ?);
    """;
            try (var ps = con.prepareStatement(sql)) {
                for (var entry : entries) {
                    ps.setObject(1, entry.subId);
                    ps.setObject(2, entry.refId);
                    ps.setTimestamp(3, Timestamp.from(entry.tv));
                    ps.setString(4, entry.ballast2);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    private DataEntry newDataEntry(int ix, long prefix, Instant tv) {
        var de = new DataEntry();
        Instant idInstant = tv.plus(ix, ChronoUnit.SECONDS);
        de.mainId = newId(prefix, idInstant);
        de.subId = newId(prefix, idInstant);
        de.refId = newId(prefix, idInstant);
        de.tv = idInstant;
        de.ballast1 = newBallast();
        de.ballast2 = newBallast();
        return de;
    }

    private Instant newTv(LocalDate dt) {
        ZonedDateTime tv = dt.atStartOfDay(timeZone);
        long seconds = ThreadLocalRandom.current().nextLong(0L, 60L * 60L * 24L);
        tv = tv.plus(seconds, ChronoUnit.SECONDS);
        return tv.toInstant();
    }

    private String newBallast() {
        StringBuilder sb = new StringBuilder();
        sb.append(getBallastLine());
        while (sb.length() < 500) {
            sb.append(", ");
            sb.append(getBallastLine());
        }
        return sb.toString();
    }

    private String getBallastLine() {
        int pos = ThreadLocalRandom.current().nextInt(0, ballastLines.size());
        return ballastLines.get(pos);
    }

    private long newPrefix() {
        return keyGen.nextPrefix();
    }

    private UUID newId(long prefix, Instant instant) {
        if (config.isUuidV8()) {
            return keyGen.nextValue(prefix, instant);
        }
        // UUIDv4 with a shared prefix applied
        UUID v = UUID.randomUUID();
        long mask = keyGen.getPrefixMask();
        long msb = v.getMostSignificantBits();
        msb &= ~mask;
        msb |= prefix & mask;
        return new UUID(msb, v.getLeastSignificantBits());
    }

    private static ArrayList<String> readBallastLines(String fname) {
        if (fname == null) {
            return new ArrayList<>();
        }
        ArrayList<String> lines = new ArrayList<>();
        try {
            var temp = Files.readAllLines(Path.of(fname), StandardCharsets.UTF_8);
            temp.stream()
                    .filter(v -> (v != null) && v.length() > 1)
                    .distinct()
                    .sorted()
                    .forEach(v -> lines.add(v));
        } catch (IOException ix) {
            throw new RuntimeException("Failed to read file " + fname, ix);
        }
        return lines;
    }

    private int runWithRetry(boolean readonly, ExConsumer<Connection> action) {
        Throwable reason = null;
        for (int retryCount = 0; retryCount < config.getRetryCount() + 1; ++retryCount) {
            try (var conn = ds.getConnection()) {
                try {
                    if (readonly) {
                        conn.setReadOnly(true);
                    }
                    action.accept(conn);
                    conn.commit();
                } finally {
                    if (readonly) {
                        try {
                            conn.setReadOnly(false);
                        } catch (SQLException ex) {
                            LOG.warn("Failed to re-set the read only state", ex);
                        }
                    }
                }
                return retryCount;
            } catch (Exception ex) {
                if (ex instanceof YdbRetryableException
                        || ex instanceof YdbConditionallyRetryableException) {
                    reason = ex;
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextLong(100L, 500L));
                    } catch (InterruptedException ix) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    throw new RuntimeException("Fill iteration failed: non-retryable exception", ex);
                }
            }
        }
        throw new RuntimeException("Fill iteration failed: retry count exceeded", reason);
    }

    private void waitForCompletion(ArrayList<Future<?>> tasks) {
        var lastReported = Instant.now();
        while (true) {
            int completedCount = (int) tasks.stream()
                    .filter(f -> f.isDone() || f.isCancelled())
                    .count();
            if (completedCount >= tasks.size()) {
                break;
            }
            var now = Instant.now();
            if (lastReported.until(now, ChronoUnit.SECONDS) >= 10L) {
                lastReported = now;
                long ic = itemsCompleted.get();
                long ie = itemsExpected.get();
                double pc = ((double) ic) * 100.0 / ((double) ie);
                var pcs = String.format("%02.2f", pc);
                LOG.info("Progress {} percent ({} / {} parts, {}M rows), running {} tasks (completed {} / {} tasks)",
                        pcs, ic, ie, rowsCompleted.get() / 1000000L,
                        tasksRunning.get(), completedCount, tasks.size());
            }
            try {
                Thread.sleep(300L);
            } catch (InterruptedException ix) {
            }
        }
    }

    public void actionLayout() {
        runWithRetry(true, conn -> showLayout(conn));
    }

    private void showLayout(Connection conn) throws Exception {
        byte[] input = new byte[16];
        for (int i = 0; i < input.length; ++i) {
            input[i] = (byte) (i + 1);
        }
        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (input[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (input[i] & 0xff);
        }
        long msb2 = UuidKeyGen.reorderForYdb(msb);
        String sql = """
                     SELECT ToBytes(?), ToBytes(?);
                     """;
        try (var ps = conn.prepareStatement(sql)) {
            ps.setObject(1, new UUID(msb, lsb));
            ps.setObject(2, new UUID(msb2, lsb));
            try (var rs = ps.executeQuery()) {
                if (rs.next()) {
                    var out1 = rs.getBytes(1);
                    var out2 = rs.getBytes(2);
                    LOG.info("INP: {}", HexFormat.of().formatHex(input));
                    LOG.info("OU1: {}", HexFormat.of().formatHex(out1));
                    LOG.info("OU2: {}", HexFormat.of().formatHex(out2));
                }
            }
        }
    }

    public void actionOrder() {
        runWithRetry(false, conn -> showOrder(conn));
    }

    private void showOrder(Connection conn) throws Exception {
        // CREATE TABLE uuid_order_test(a Uuid NOT NULL, b Int32 NOT NULL, PRIMARY KEY(a))
        String sql = """
                     UPSERT INTO uuid_order_test(a,b) VALUES(?,?);
                     """;
        try (var ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 16; ++i) {
                ps.setObject(1, makeUuid(i));
                ps.setInt(2, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static final long DIGITOR[] = new long[]{
        0x0000000000000001L,
        0x0000000000000200L,
        0x0000000000030000L,
        0x0000000004000000L,
        0x0000000500000000L,
        0x0000060000000000L,
        0x0007000000000000L,
        0x0800000000000000L
    };

    private UUID makeUuid(int digit) {
        long msb = 0;
        long lsb = 0;
        if (digit >= 0 && digit < 8) {
            lsb = DIGITOR[digit];
        } else if (digit >= 8 && digit < 16) {
            msb = DIGITOR[digit - 8];
        } else {
            throw new IllegalArgumentException();
        }
        msb = UuidKeyGen.reorderForYdb(msb);
        return new UUID(msb, lsb);
    }

    public static Config readConfig(String fname) throws Exception {
        var props = new Properties();
        try (var fis = new FileInputStream(fname)) {
            props.loadFromXML(fis);
        }
        String v;
        var config = new Config();
        config.setUrl(props.getProperty("ydb.url"));
        config.setLogin(props.getProperty("ydb.user"));
        config.setPassword(props.getProperty("ydb.password"));
        config.setDdlFile(props.getProperty("ddl.file"));
        config.setBallastFile(props.getProperty("gen.ballast.file"));
        v = props.getProperty("gen.uuid.v8");
        if (v != null) {
            config.setUuidV8(Boolean.parseBoolean(v));
        }
        v = props.getProperty("gen.scale");
        if (v != null) {
            config.setGeneratorScale(Integer.parseInt(v));
        }
        v = props.getProperty("gen.start");
        if (v != null) {
            config.setGeneratorStart(LocalDate.parse(v));
        }
        v = props.getProperty("gen.finish");
        if (v != null) {
            config.setGeneratorFinish(LocalDate.parse(v));
        }
        v = props.getProperty("gen.threads");
        if (v != null) {
            config.setGeneratorThreads(Integer.parseInt(v));
        }
        v = props.getProperty("test.threads");
        if (v != null) {
            config.setTestThreads(Integer.parseInt(v));
        }
        v = props.getProperty("test.rows");
        if (v != null) {
            config.setTestRows(Integer.parseInt(v));
        }
        v = props.getProperty("test.day");
        if (v != null) {
            config.setTestDay(LocalDate.parse(v));
        }
        v = props.getProperty("test.iterations");
        if (v != null) {
            config.setTestIterations(Integer.parseInt(v));
        }
        v = props.getProperty("retry.count");
        if (v != null) {
            config.setRetryCount(Integer.parseInt(v));
        }
        return config;
    }

    @FunctionalInterface
    public static interface ExConsumer<T> {

        void accept(T t) throws Exception;
    }

    public static final class DataEntry {

        UUID mainId;
        UUID subId;
        UUID refId;
        Instant tv;
        String ballast1;
        String ballast2;
    }

    public enum Action {
        INIT,
        FILL,
        TEST,
        CLEAN,
        PRINT,
        LAYOUT,
        ORDER
    }

    public static final class Config {

        private String url;
        private String login;
        private String password;
        private String ddlFile;
        private String ballastFile;
        private int generatorScale = 1;
        private LocalDate generatorStart;
        private LocalDate generatorFinish;
        private int generatorThreads = 4;
        private int testThreads = 4;
        private int testRows = 10;
        private LocalDate testDay;
        private int testIterations = 100;
        private int retryCount = 10;
        private boolean uuidV8 = true;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getLogin() {
            return login;
        }

        public void setLogin(String login) {
            this.login = login;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDdlFile() {
            return ddlFile;
        }

        public void setDdlFile(String ddlFile) {
            this.ddlFile = ddlFile;
        }

        public String getBallastFile() {
            return ballastFile;
        }

        public void setBallastFile(String ballastFile) {
            this.ballastFile = ballastFile;
        }

        public int getGeneratorScale() {
            return generatorScale;
        }

        public void setGeneratorScale(int generatorScale) {
            this.generatorScale = generatorScale;
        }

        public LocalDate getGeneratorStart() {
            return generatorStart;
        }

        public void setGeneratorStart(LocalDate generatorStart) {
            this.generatorStart = generatorStart;
        }

        public LocalDate getGeneratorFinish() {
            return generatorFinish;
        }

        public void setGeneratorFinish(LocalDate generatorFinish) {
            this.generatorFinish = generatorFinish;
        }

        public int getGeneratorThreads() {
            return generatorThreads;
        }

        public void setGeneratorThreads(int generatorThreads) {
            this.generatorThreads = generatorThreads;
        }

        public int getTestThreads() {
            return testThreads;
        }

        public void setTestThreads(int testThreads) {
            this.testThreads = testThreads;
        }

        public int getTestRows() {
            return testRows;
        }

        public void setTestRows(int testRows) {
            this.testRows = testRows;
        }

        public LocalDate getTestDay() {
            return testDay;
        }

        public void setTestDay(LocalDate testDay) {
            this.testDay = testDay;
        }

        public int getTestIterations() {
            return testIterations;
        }

        public void setTestIterations(int testIterations) {
            this.testIterations = testIterations;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void setRetryCount(int retryCount) {
            this.retryCount = retryCount;
        }

        public boolean isUuidV8() {
            return uuidV8;
        }

        public void setUuidV8(boolean uuidV8) {
            this.uuidV8 = uuidV8;
        }

    }

}
