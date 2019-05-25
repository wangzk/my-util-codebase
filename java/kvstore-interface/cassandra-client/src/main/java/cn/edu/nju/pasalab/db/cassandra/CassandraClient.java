package cn.edu.nju.pasalab.db.cassandra;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class CassandraClient extends BasicKVDatabaseClient {

    public static final String CONF_KEYSPACE_NAME = "cassandra.keyspace.name";
    public static final String DEFAULT_KEYSPACE_NAME = "mydata";
    public static final String CONF_TABLE_NAME = "cassandra.table.name";
    public static final String DEFAULT_TABLE_NAME = "datatable";
    public static final String CONF_COLUMN_KEY = "cassandra.column.key";
    public static final String DEFAULT_COLUMN_KEY = "key";
    public static final String CONF_COLUMN_VALUE = "cassandra.column.value";
    public static final String DEFAULT_COLUMN_VALUE = "value";
    public static final String CONF_USE_HASHED_KEY = "cassandra.use.hashed.key";
    public static final String DEFAULT_USE_HASHED_KEY = "false";
    public static final String CONF_CONTACT_POINTS = "cassandra.contact.points";
    public static final String DEFAULT_CONTACT_POINTS = "127.0.0.1";


    private List<String> contactPoints;
    private String keyspace;
    private String tableName;
    private String qulifiedTableName;
    private String keyColumnName;
    private String valueColumnName;
    private boolean useHashedKey;

    private Cluster cluster;
    private Session session;

    private Logger logger = Logger.getLogger(this.getClass().getName());

    @Override
    public byte[] get(byte[] key) throws Exception {
        return new byte[0];
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {

    }

    @Override
    public void close() throws Exception {

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
    }

    @Override
    public void clearDB() throws Exception {
        if (this.cluster.getMetadata().getKeyspace(this.keyspace) == null) {
            this.session.execute("CREATE KEYSPACE " + this.keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor': 3}");
            logger.info("Create the keyspace");
        }
        if (this.cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.tableName) != null) {
            logger.info("Drop the old table.");
            this.session.execute("DROP TABLE " + this.qulifiedTableName);
        }
        this.session.execute("CREATE TABLE " + this.qulifiedTableName +
                "( " + this.keyColumnName + " blob PRIMARY KEY, " + this.valueColumnName + " blob)");
        logger.info("Table " + this.tableName + " re-created.");
    }

    @Override
    public void createDB() throws Exception {
        clearDB();
    }
}
