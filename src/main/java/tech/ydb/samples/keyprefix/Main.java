package tech.ydb.samples.keyprefix;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
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
    private final TextKeyGen keyGen;
    private final ArrayList<String> ballastLines;
    private final ZoneId timeZone;
    private final AtomicLong completed = new AtomicLong();

    public Main(Config sc) {
        this.config = sc;
        this.ds = createDataSource(sc);
        this.keyGen = new TextKeyGen();
        this.ballastLines = readBallastLines(sc.getBallastFile());
        this.timeZone = ZoneId.of("Europe/Moscow");
    }

    @Override
    public void close() {
        ds.close();
    }

    public void init() throws Exception {
        LOG.info("Init started...");
        runDdlScript();
        LOG.info("Init successfull!");
    }

    public void clean() throws Exception {
        LOG.info("Cleanup started...");
        dropTables();
        LOG.info("Cleanup successfull!");
    }

    public void fill() throws Exception {
        LOG.info("Fill started...");
        completed.set(0L);
        try (var service = Executors.newFixedThreadPool(config.getGeneratorThreads())) {
            var tasks = new ArrayList<Future<?>>();
            // one task per date
            LocalDate current = config.getGeneratorStart();
            while (!current.isAfter(config.getGeneratorFinish())) {
                LocalDate dt = current;
                var task = service.submit(() -> fillDate(dt));
                tasks.add(task);
                current = current.plusDays(1);
            }
            waitForCompletion(tasks);
        }
        LOG.info("Fill successful!");
    }

    public void test() throws Exception {
        LOG.info("Test started...");
        LOG.info("Test successful!");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            LOG.info("Two arguments are expected: config-file.xml { INIT | FILL | TEST | CLEAN }");
            System.exit(2);
        }
        try {
            var action = Action.valueOf(args[1]);
            LOG.info("Reading configuration {}...", args[0]);
            var config = readConfig(args[0]);
            try (var m = new Main(config)) {
                switch (action) {
                    case INIT -> {
                        m.init();
                    }
                    case FILL -> {
                        m.fill();
                    }
                    case TEST -> {
                        m.test();
                    }
                    case CLEAN -> {
                        m.clean();
                    }
                }
            }
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
        v = props.getProperty("gen.ids.smart");
        if (v != null) {
            config.setGeneratorSmartIds(Boolean.parseBoolean(v));
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
        v = props.getProperty("retry.count");
        if (v != null) {
            config.setRetryCount(Integer.parseInt(v));
        }
        return config;
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

    private void fillDate(LocalDate dt) {
        LOG.info("Filling data for {}...", dt);
        for (int i = 0; i < config.getGeneratorScale(); ++i) {
            fillDateStepRetry(dt, i);
            completed.incrementAndGet();
        }
        LOG.info("Completed filling data for {}.", dt);
    }

    private int fillDateStepRetry(LocalDate dt, int iteration) {
        Throwable reason = null;
        for (int retryCount = 0; retryCount < config.getRetryCount() + 1; ++retryCount) {
            try (var conn = ds.getConnection()) {
                fillDateStep(conn, dt, iteration);
                return retryCount;
            } catch (Exception ex) {
                if (ex instanceof YdbRetryableException
                        || ex instanceof YdbConditionallyRetryableException) {
                    reason = ex;
                } else {
                    throw new RuntimeException("Fill iteration failed: non-retryable exception", ex);
                }
            }
        }
        throw new RuntimeException("Fill iteration failed: retry count exceeded", reason);
    }

    private void fillDateStep(Connection con, LocalDate dt, int iteration) throws Exception {
        // 5 iterations for 200 lines each
        for (int i = 0; i < 5; ++i) {
            long prefix = newPrefix();
            var entries = IntStream.range(0, 200)
                    .mapToObj(ix -> newDataEntry(prefix, dt))
                    .toList();
            String sql = """
    INSERT INTO `key_prefix_demo/main`(id, collection_id, tv, ballast1)
    VALUES (?, ?, ?, ?);
    """;
            try (var ps = con.prepareStatement(sql)) {
                for (var entry : entries) {
                    ps.setString(1, entry.mainId);
                    ps.setString(2, entry.refId);
                    ps.setTimestamp(3, Timestamp.from(entry.tv));
                    ps.setString(4, entry.ballast1);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            sql = """
    INSERT INTO `key_prefix_demo/sub`(id, ref_id, tv, ballast2)
    VALUES (?, ?, ?, ?);
    """;
            try (var ps = con.prepareStatement(sql)) {
                for (var entry : entries) {
                    ps.setString(1, entry.subId);
                    ps.setString(2, entry.refId);
                    ps.setTimestamp(3, Timestamp.from(entry.tv));
                    ps.setString(4, entry.ballast2);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        con.commit();
    }

    private DataEntry newDataEntry(long prefix, LocalDate dt) {
        var de = new DataEntry();
        de.mainId = newId(prefix, dt);
        de.subId = newId(prefix, dt);
        de.refId = newId(prefix, dt);
        de.tv = newTv(dt);
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
        while (sb.length() < 1000) {
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

    private String newId(long prefix, LocalDate dt) {
        if (config.isGeneratorSmartIds()) {
            return keyGen.nextValue(prefix, dt);
        } else {
            UUID uuid = UUID.randomUUID();
            ByteBuffer byteArray = ByteBuffer.allocate(16);
            byteArray.putLong(uuid.getMostSignificantBits());
            byteArray.putLong(uuid.getLeastSignificantBits());
            String temp1 = Base64.getUrlEncoder()
                    .encodeToString(byteArray.array())
                    .substring(0, 22);
            byteArray = ByteBuffer.allocate(8);
            byteArray.putLong(prefix);
            String temp2 = Base64.getUrlEncoder()
                    .encodeToString(byteArray.array());
            return temp2.substring(0, 4) + temp1.substring(4);
        }
    }

    private static ArrayList<String> readBallastLines(String fname) {
        if (fname == null) {
            return new ArrayList<>();
        }
        var lines = new ArrayList<String>();
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
                LOG.info("Completed {} of {} ({} of {} parts)",
                        completedCount, tasks.size(),
                        completed.get(), 1000L * tasks.size());
            }
            try {
                Thread.sleep(200L);
            } catch (InterruptedException ix) {
            }
        }
    }

    public static final class DataEntry {

        String mainId;
        String subId;
        String refId;
        Instant tv;
        String ballast1;
        String ballast2;
    }

    public enum Action {
        INIT,
        FILL,
        TEST,
        CLEAN
    }

    public static final class Config {

        private String url;
        private String login;
        private String password;
        private String ddlFile;
        private String ballastFile;
        private boolean generatorSmartIds = true;
        private int generatorScale = 1;
        private LocalDate generatorStart;
        private LocalDate generatorFinish;
        private int generatorThreads = 4;
        private int testThreads = 4;
        private int retryCount = 10;

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

        public boolean isGeneratorSmartIds() {
            return generatorSmartIds;
        }

        public void setGeneratorSmartIds(boolean generatorSmartIds) {
            this.generatorSmartIds = generatorSmartIds;
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

        public int getRetryCount() {
            return retryCount;
        }

        public void setRetryCount(int retryCount) {
            this.retryCount = retryCount;
        }

    }

}
