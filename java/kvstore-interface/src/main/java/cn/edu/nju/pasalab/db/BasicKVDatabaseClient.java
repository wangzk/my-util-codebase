package cn.edu.nju.pasalab.db;

import java.io.IOException;
import java.util.Map;

/**
 * The abstract class for a simple key-value database client.
 */
public abstract class BasicKVDatabaseClient {

    public abstract byte[] get(byte key[]) throws Exception;
    public abstract void put(byte key[], byte value[]) throws Exception;

    /**
     * Get the values of a group of keys
     * @param keys
     * @return the values of the keys
     * @throws Exception
     */
    public byte[][] getAll(byte keys[][]) throws Exception{
        byte result[][] = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            result[i] = get(keys[i]);
        }
        return result;
    }

    /**
     * Set the key-value pairs in batch
     * @param keys
     * @param values
     * @throws Exception
     */
    public void putAll(byte keys[][], byte values[][]) throws Exception {
        assert keys.length == values.length;
        for (int i = 0; i < keys.length; i++) {
            put(keys[i], values[i]);
        }
    }

    /**
     * Close the database connections.
     */
    public abstract void close() throws Exception;

    /**
     * Connect to the underlying database
     * @throws Exception database connection exception
     */
    public abstract void connect() throws Exception;

    /**
     * Clear the contents in the database
     */
    public abstract void clearDB() throws Exception;
}
