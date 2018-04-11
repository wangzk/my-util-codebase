package cn.edu.nju.pasalab.db;

import cn.edu.nju.pasalab.conf.ProcessLevelConf;
import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by wangzhaokang on 4/11/18.
 */
public final class ShardedRedisClusterClient extends BasicKVDatabaseClient {

    public static final String CONF_POOL_SIZE = "redis.connection.pool.size";
    public static final String CONF_DB_ID = "redis.db.index";
    public static final String CONF_HOSTS_LIST = "redis.hosts.list";
    public static final int DEFAULT_TIME_OUT_IN_SEC = 36000;
    public static final int DEFAULT_REDIS_PORT = 6379;


    private List<String> hosts;
    private int databaseID;
    private int poolSize = 4;
    private ShardedJedisPool pool;
    private Logger logger = Logger.getLogger(ShardedRedisClusterClient.class.getName());

    private static ShardedRedisClusterClient processLevelSingletonClient;

    /**
     * Get a thread-safe process-level Redis client.
     * @return The process-level thread-safe client. This client will be shared among all threads in the JVM. The returned client is already connected to the database.
     * @throws Exception
     */
    public static ShardedRedisClusterClient getProcessLevelClient() throws Exception{
        if (processLevelSingletonClient != null) return processLevelSingletonClient;
        else {
            initProcessLevelClient();
            return processLevelSingletonClient;
        }
    }

    private static synchronized void initProcessLevelClient() throws Exception {
        if (processLevelSingletonClient == null) {
            Properties pasaProperties = ProcessLevelConf.getPasaConf();
            String hostsString = pasaProperties.getProperty(CONF_HOSTS_LIST, "localhost");
            int dbID = Integer.parseInt(pasaProperties.getProperty(CONF_DB_ID, "0"));
            int poolSize = Integer.parseInt(pasaProperties.getProperty(CONF_POOL_SIZE, "4"));
            List<String> hosts = Arrays.asList(hostsString.split(","));
            processLevelSingletonClient = new ShardedRedisClusterClient(hosts, dbID, poolSize);
            // connect
            processLevelSingletonClient.connect();
        }
    }

    public ShardedRedisClusterClient(List<String> redisHosts, int databaseID, int poolSize) {
        this.hosts = new ArrayList<>();
        this.hosts.addAll(redisHosts);
        this.databaseID = databaseID;
        this.poolSize = poolSize;
    }


    @Override
    public byte[] get(byte[] key) throws Exception {
        byte[] value = null;
        try(ShardedJedis jedis = pool.getResource()) {
            value = jedis.get(key);
        }
        return value;
    }

    @Override
    public byte[][] getAll(byte keys[][]) throws Exception{
        byte results[][] = new byte[keys.length][];
        try(ShardedJedis jedis = pool.getResource()){
            ShardedJedisPipeline pipeline = jedis.pipelined();
            Response<byte[]> responses[] = new Response[keys.length];
            for (int i = 0; i < keys.length; i++) {
                responses[i] = pipeline.get(keys[i]);
            }
            pipeline.sync();
            for (int i = 0; i < keys.length; i++) {
                results[i] = responses[i].get();
            }
        }
        return results;
    }


    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        try(ShardedJedis jedis = pool.getResource()) {
            jedis.set(key, value);
        }
    }

    @Override
    public void putAll(byte keys[][], byte values[][]) throws Exception {
        assert keys.length == values.length;
        try(ShardedJedis jedis = pool.getResource()){
            ShardedJedisPipeline pipeline = jedis.pipelined();
            for (int i = 0; i < keys.length; i++) {
                pipeline.set(keys[i], values[i]);
            }
            pipeline.sync();
        }
    }

    @Override
    public void close() throws Exception{
        logger.info("Start closing connection pool...");
        pool.close();
        this.pool = null;
        logger.info("Connection pool closed.");
    }

    @Override
    public void connect() throws Exception {
        logger.info("Start connecting...");
        // Prepare Jedis shards
        List<JedisShardInfo> shards =
            hosts.stream().map(host -> {
                String hostURI = String.format("redis://%s:%d/%d", host, DEFAULT_REDIS_PORT, databaseID);
                JedisShardInfo si = new JedisShardInfo(hostURI);
                si.setPassword(null);
                si.setConnectionTimeout(DEFAULT_TIME_OUT_IN_SEC);
                return si;
            }).collect(Collectors.toList());
        logger.info("Shards:" + shards);
        // Prepare the jedis pool
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(poolSize);
        this.pool = new ShardedJedisPool(jedisPoolConfig, shards);
        logger.info("Connection established.");
    }

    @Override
    public void clearDB() throws Exception {
       List<CompletableFuture> futures = hosts.stream().map(host -> {
           return CompletableFuture.runAsync(() -> {
               Jedis jedis = new Jedis(host, DEFAULT_REDIS_PORT, DEFAULT_TIME_OUT_IN_SEC);
               jedis.select(databaseID);
               jedis.flushDB();
               jedis.close();
           });
       }).collect(Collectors.toList());
       CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
    }
}
