package cn.edu.nju.pasalab.db.cache;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * Created by wzk on 18-4-28.
 */
public class CachedClient extends BasicKVDatabaseClient {

    static public final String CONF_CACHE_CAPACITY = "cache.capacity.in.gb"; // in Gbytes
    static public final String DEFAULT_CACHE_CAPACITY = "1"; // in Gbytes
    static public final String CONF_CACHE_STATS_FILE_PATH = "cache.stats.file.path"; // store cache stats
    static public final String DEFAULT_CACHE_STATS_FILE_PATH = "/tmp/cache.stats";
    static public final String CONF_CACHE_COMPACT_FACTOR = "cache.compact.factor"; // 0-1
    static public final String DEFAULT_CACHE_COMPACT_FACTOR = "0.2";
    static public final String CONF_CACHE_EXPIRE_PERIOD = "cache.expire.period.in.sec"; // in second
    static public final String DEFAULT_CACHE_EXPIRE_PERIOD = "10";
    static public final String CONF_CACHE_EXPIRE_NUM_THREADS = "cache.expire.num.thread";
    static public final String DEFAULT_CACHE_EXPIRE_NUM_THREADS = "4";
    static public final String CONF_HMAP_CONCURRENCY = "cache.hmap.concurrency";
    static public final String DEFAULT_HMAP_CONCURRENCY = "4";
    static public final String CONF_DB_BACKEND_CLASS_NAME = "cache.db.backend.class.name"; // required, no default!

    private static Thread cacheStatsReportThread = null;


    private BasicKVDatabaseClient db;
    private HTreeMap<byte[], byte[]> cache;
    private ScheduledExecutorService expireExecutorService;
    private long cacheCapacityInGB = 1;
    private String statsFilePath;
    private float compactFactor;
    private long expirePeriod;
    private int expireNumThread;
    private int hmapConcurrency;
    private String dbClassName;
    private long queryCount = 0L;
    private long hitCount = 0L;
    private Logger logger = Logger.getLogger(this.getClass().getName());


    /**
     * @param key
     * @return the value. Return null if the key does not exist.
     * @throws Exception
     */
    @Override
    public byte[] get(byte[] key) throws Exception {
        assert cache != null;
        queryCount++;
        byte[] result = cache.get(key);
        if (result == null) {
            result = db.get(key);
            if (result != null)
                cache.put(key, result);
        } else {
            hitCount++;
        }
        return result;
    }

    @Override
    public byte[][] getAll(byte keys[][]) throws Exception{
        byte[][] results = new byte[keys.length][];
        IntArrayList queryKeysIDs = new IntArrayList();

        for (int i = 0; i < keys.length; i++) {
            queryCount++;
            byte[] result = cache.get(keys[i]);
            if (result == null) {
                queryKeysIDs.add(i);
            } else {
                results[i] = result;
                hitCount++;
            }
        }
        byte[][] queryKeys = new byte[queryKeysIDs.size()][];
        for (int i = 0; i < queryKeysIDs.size(); i++) {
            queryKeys[i] = keys[queryKeysIDs.getInt(i)];
        }
        byte[][] queryResults = db.getAll(queryKeys);
        for (int i = 0; i < queryKeysIDs.size(); i++) {
            int kID = queryKeysIDs.getInt(i);
            results[kID] = queryResults[i];
            if (queryResults[i] != null)
                cache.put(queryKeys[i], queryResults[i]);
        }
        return results;
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        db.put(key, value);
    }

    @Override
    public void putAll(byte[][] keys, byte[][] values) throws Exception {
        db.putAll(keys, values);
    }

    /**
     * Close the database connections.
     */
    @Override
    public void close() throws Exception {
        cache.clear();
        cache.close();
        db.close();
        cacheStatsReportThread.interrupt();
    }

    /**
     * Clear the contents in the database
     */
    @Override
    public void clearDB() throws Exception {
        cache.clear();
        db.clearDB();
    }

    private void loadConfigurations(Properties conf) {
        String capacityString = conf.getProperty(CONF_CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY);
        this.cacheCapacityInGB = Integer.valueOf(capacityString);
        this.statsFilePath = conf.getProperty(CONF_CACHE_STATS_FILE_PATH, DEFAULT_CACHE_STATS_FILE_PATH);
        this.compactFactor = Float.valueOf(conf.getProperty(CONF_CACHE_COMPACT_FACTOR, DEFAULT_CACHE_COMPACT_FACTOR));
        this.expirePeriod = Long.valueOf(conf.getProperty(CONF_CACHE_EXPIRE_PERIOD, DEFAULT_CACHE_EXPIRE_PERIOD));
        this.expireNumThread = Integer.valueOf(conf.getProperty(CONF_CACHE_EXPIRE_NUM_THREADS, DEFAULT_CACHE_EXPIRE_NUM_THREADS));
        this.hmapConcurrency = Integer.valueOf(conf.getProperty(CONF_HMAP_CONCURRENCY, DEFAULT_HMAP_CONCURRENCY));
        this.dbClassName = conf.getProperty(CONF_DB_BACKEND_CLASS_NAME);
        logger.info("Get configurations:" + conf);
    }

    /**
     * Connect to the underlying database
     *
     * @param conf Database connection-related configurations
     * @throws Exception database connection exception
     */
    @Override
    public void connect(Properties conf) throws Exception {
        loadConfigurations(conf);
        // Connect database
        logger.info("Database backend:" + dbClassName);
        Class dbClass = Class.forName(dbClassName);
        this.db = (BasicKVDatabaseClient) dbClass.newInstance();
        db.connect(conf);
        // Create cache
        expireExecutorService = Executors.newScheduledThreadPool(this.expireNumThread);
        // create the cache
        cache = DBMaker.memoryShardedHashMap(this.hmapConcurrency)
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .expireMaxSize(cacheCapacityInGB * 1024L * 1024L * 1024L)
                .expireAfterGet()
                .expireCompactThreshold(this.compactFactor)
                .expireExecutor(expireExecutorService)
                .expireExecutorPeriod(this.expirePeriod)
                .create();

        cacheStatsReportThread = new Thread(new CacheStatsReportRunnable(expirePeriod));
        cacheStatsReportThread.setDaemon(true);
        cacheStatsReportThread.setName("Cache Stats Reporter");
        cacheStatsReportThread.start();

    }



    private class CacheStatsReportRunnable implements Runnable {

        private long sleepTimeInSecond = 2;

        public CacheStatsReportRunnable() {
        }

        public CacheStatsReportRunnable(long sleepTimeInSecond) {
            this.sleepTimeInSecond = sleepTimeInSecond;
        }

        private void writeStatsToFile() {
            try {
                PrintWriter writer = new PrintWriter(new File(statsFilePath));
                long missCount = queryCount - hitCount;
                writer.println(String.format("CacheStats{hitCount=%d, missCount=%d, }", hitCount, missCount));
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }


        @Override
        public void run() {
            while (true) {
                writeStatsToFile();
                try {
                    Thread.sleep(sleepTimeInSecond * 1000, 0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
