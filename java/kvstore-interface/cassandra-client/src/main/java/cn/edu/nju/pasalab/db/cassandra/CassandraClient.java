package cn.edu.nju.pasalab.db.cassandra;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class CassandraClient extends BasicKVDatabaseClient {

    public static final String CONF_KEYSPACE_NAME = "cassandra.keyspace.name";
    public static final String DEFAULT_KEYSPACE_NAME = "mydata";
    public static final String CONF_TABLE_NAME = "cassandra.table.name";
    public static final String DEFAULT_TABLE_NAME = "datatable";
    public static final String CONF_COLUMN_KEY = "cassandra.column.key";
    public static final String DEFAULT_COLUMN_KEY = "mykey";
    public static final String CONF_COLUMN_VALUE = "cassandra.column.value";
    public static final String DEFAULT_COLUMN_VALUE = "myvalue";
    public static final String CONF_USE_HASHED_KEY = "cassandra.use.hashed.key";
    public static final String DEFAULT_USE_HASHED_KEY = "false";
    public static final String CONF_CONTACT_POINTS = "cassandra.contact.points";
    public static final String DEFAULT_CONTACT_POINTS = "127.0.0.1";
    public static final String CONF_CONCURRENT_QUERY = "cassandra.concurrent.query";
    public static final String DEFAULT_CONCURRENT_QUERY = "1000";


    private List<String> contactPoints;
    private String keyspace;
    private String tableName;
    private String qulifiedTableName;
    private String keyColumnName;
    private String valueColumnName;
    private boolean useHashedKey;
    private int numConcurrentQuery;

    private Cluster cluster;
    private Session session;
    private PreparedStatement putStatement;
    private PreparedStatement getStatement;
    private Semaphore querySemaphore;
    private FutureSemaphoreCallback callback;

    private Logger logger = Logger.getLogger(this.getClass().getName());

    @Override
    public byte[] get(byte[] key) throws Exception {
       if (key == null)
            return null;
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        BoundStatement statement = getStatement.bind(keyBuffer);
        ResultSet resultSet = session.execute(statement);
        Row row = resultSet.one();
        if (row == null || row.isNull(0))
            return null;
        ByteBuffer buf = row.getBytes(0);
        return buf.array();
    }

    @Override
    public byte[][] getAll(byte[][] keys) throws Exception {
       byte[][] result = new byte[keys.length][];
        List<ResultSetFuture> futures = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            ByteBuffer keyBuffer = ByteBuffer.wrap(keys[i]);
            BoundStatement s = getStatement.bind(keyBuffer);
            querySemaphore.acquire();
            ResultSetFuture resultFuture = session.executeAsync(s);
            Futures.addCallback(resultFuture, this.callback);
            futures.add(resultFuture);
        }
        List<ResultSet> resultSets = futures.stream().map(f -> f.getUninterruptibly()).collect(Collectors.toList());
        Iterator<ResultSet> iterator = resultSets.iterator();
        for (int i = 0; i < keys.length; i++) {
            ResultSet r = iterator.next();
            assert r != null;
            Row row = r.one();
            if (row != null && !row.isNull(0)) {
                result[i] = row.getBytes(0).array();
            }
        }
        return result;
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        ByteBuffer valueBuffer = ByteBuffer.wrap(value);
        BoundStatement statement = putStatement.bind(keyBuffer, valueBuffer);
        session.execute(statement);
    }

    public void putAll(byte[][] keys, byte[][] values) throws Exception {
        List<ResultSetFuture> futures = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            ByteBuffer keyBuffer = ByteBuffer.wrap(keys[i]);
            ByteBuffer valueBuffer = ByteBuffer.wrap(values[i]);
            BoundStatement s = putStatement.bind(keyBuffer, valueBuffer);
            querySemaphore.acquire();
            ResultSetFuture future = this.session.executeAsync(s);
            Futures.addCallback(future, this.callback);
            futures.add(future);
        }
        futures.forEach(f -> f.getUninterruptibly());
    }

    @Override
    public void close() throws Exception {
        session.close();
        cluster.close();
    }


    private void loadConfiguration(Properties conf) {
        String contactPointsString = conf.getProperty(CONF_CONTACT_POINTS, DEFAULT_CONTACT_POINTS);
        this.contactPoints = Arrays.stream(contactPointsString.split(",")).collect(Collectors.toList());
        this.keyspace = conf.getProperty(CONF_KEYSPACE_NAME, DEFAULT_KEYSPACE_NAME);
        this.tableName = conf.getProperty(CONF_TABLE_NAME, DEFAULT_TABLE_NAME);
        this.qulifiedTableName = this.keyspace + "." + this.tableName;
        this.keyColumnName = conf.getProperty(CONF_COLUMN_KEY, DEFAULT_COLUMN_KEY);
        this.valueColumnName = conf.getProperty(CONF_COLUMN_VALUE, DEFAULT_COLUMN_VALUE);
        this.useHashedKey = Boolean.parseBoolean(conf.getProperty(CONF_USE_HASHED_KEY, DEFAULT_USE_HASHED_KEY));
        this.numConcurrentQuery = Integer.parseInt(conf.getProperty(CONF_CONCURRENT_QUERY, DEFAULT_CONCURRENT_QUERY));
    }

    private void initKeyspaceAndTable() throws Exception {
        if (this.cluster.getMetadata().getKeyspace(this.keyspace) == null) {
            this.session.execute("CREATE KEYSPACE " + this.keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor': 3}");
            logger.info("Create the keyspace");
        }
        if (this.cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.tableName) == null) {
            createTable();
            waitForTableCreation();
            logger.info("Create the table");
        }
    }

    @Override
    public void connect(Properties conf) throws Exception {
        loadConfiguration(conf);
        Cluster.Builder builder = Cluster.builder();
        contactPoints.forEach(point -> {
            System.out.println(point);
            builder.addContactPoint(point);
        });
        this.cluster = builder.build();
        this.session = this.cluster.connect();
        logger.info("Cassandra database connection established.");
        initKeyspaceAndTable();
        this.querySemaphore = new Semaphore(this.numConcurrentQuery);
        this.callback = new FutureSemaphoreCallback();
        prepareStatements();
   }

    @Override
    public void clearDB() throws Exception {
        if (this.cluster.getMetadata().getKeyspace(this.keyspace) == null) {
            this.session.execute("CREATE KEYSPACE " + this.keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor': 3}");
            logger.info("Create the keyspace");
        }
        if (this.cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.tableName) != null) {
            this.session.execute("DROP TABLE " + this.qulifiedTableName);
            logger.info("Dropped the old table.");
        }
        logger.info("Start re-create the table.");
        createTable();
        waitForTableCreation();
        logger.info("Table " + this.tableName + " re-created.");
        logger.info("Now prepare statements.");
        prepareStatements();
    }

    @Override
    public void createDB() throws Exception {
        clearDB();
    }

    private void prepareStatements() {
        this.getStatement = this.session.prepare("SELECT " + this.valueColumnName
                + " FROM " + this.qulifiedTableName
                + " WHERE " + this.keyColumnName + " = ?");
        this.putStatement = this.session.prepare("INSERT INTO " + this.qulifiedTableName
                + " (" + this.keyColumnName + "," + this.valueColumnName + ") VALUES (?, ?)");
    }

    private void createTable() {
        logger.info("Try to create the table.");
        ResultSet r = this.session.execute("CREATE TABLE IF NOT EXISTS " + this.qulifiedTableName +
                "(" + this.keyColumnName + " blob PRIMARY KEY, " + this.valueColumnName + " blob);");
        logger.info(r.toString());
    }

    private void waitForTableCreation() throws Exception{
        boolean flag = false;
        int tryCount = 0;
        while (!flag && tryCount < 20) {
            if (this.cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.tableName) == null) {
                try {
                    Thread.sleep(1000);
                    tryCount++;
                    System.err.print(".");
                    createTable();
                } catch (InterruptedException e) {
                }
            } else {
                flag = true;
            }
        }
        if (!flag && this.cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.tableName) == null) {
            throw new Exception("Cannot create table!");
        }
    }

    private class FutureSemaphoreCallback implements FutureCallback<ResultSet> {

        @Override
        public void onSuccess(ResultSet rows) {
            querySemaphore.release();
        }

        @Override
        public void onFailure(Throwable throwable) {
            querySemaphore.release();
        }
    }
}
