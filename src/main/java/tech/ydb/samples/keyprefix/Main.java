package tech.ydb.samples.keyprefix;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 *
 * @author zinal
 */
public class Main implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private final Config config;
    private final HikariDataSource ds;

    public Main(Config sc) {
        this.config = sc;
        this.ds = createDataSource(sc);
    }

    @Override
    public void close() {
        ds.close();
    }

    public void init() throws Exception {
        runDdlScript();
        loadData();
    }

    public void clean() throws Exception {
        dropTables();
    }

    public void test() throws Exception {

    }

    public static void main(String[] args) {
        if (args.length != 2) {
            LOG.info("Two arguments are expected: config-file.xml { INIT | TEST | CLEAN }");
            System.exit(2);
        }
        try {
            var action = Action.valueOf(args[1]);
            LOG.info("Reading configuration {}...", args[0]);
            var config = readConfig(args[0]);
            try (var m = new Main(config)) {
                switch (action) {
                    case INIT: {
                        m.init();
                    }
                    case TEST: {
                        m.test();
                    }
                    case CLEAN: {
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
        config.setSalt(props.getProperty("gen.salt"));
        v = props.getProperty("gen.scale");
        if (v != null) {
            config.setScale(Integer.parseInt(v));
        }
        v = props.getProperty("gen.threads");
        if (v != null) {
            config.setGeneratorThreads(Integer.parseInt(v));
        }
        v = props.getProperty("test.threads");
        if (v != null) {
            config.setTestThreads(Integer.parseInt(v));
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

    private void loadData() throws Exception {

    }

    public static final class WorkerFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            int workerId = counter.getAndIncrement();
            final Thread t = new Thread(r, "YdbImporter-worker-" + workerId);
            t.setDaemon(false);
            return t;
        }
    }

    public enum Action {
        INIT,
        TEST,
        CLEAN
    }

    public static final class Config {

        private String url;
        private String login;
        private String password;
        private String ddlFile;
        private String salt;
        private int scale = 1;
        private int generatorThreads = 4;
        private int testThreads = 4;

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

        public String getSalt() {
            return salt;
        }

        public void setSalt(String salt) {
            this.salt = salt;
        }

        public int getScale() {
            return scale;
        }

        public void setScale(int scale) {
            this.scale = scale;
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

    }

}
