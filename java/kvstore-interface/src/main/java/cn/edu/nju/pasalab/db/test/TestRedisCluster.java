package cn.edu.nju.pasalab.db.test;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import cn.edu.nju.pasalab.db.ShardedRedisClusterClient;
import cn.edu.nju.pasalab.db.Utils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by wangzhaokang on 4/11/18.
 */
public class TestRedisCluster {

    //byte 数组与 int 的相互转换
    public static int byteArrayToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static void main(String args[]) throws Exception {
        Logger logger = Logger.getLogger("Test Redis Cluster");
        logger.info("Start preparing data...");
        // Prepare random data
        Set<Integer> keySet = new HashSet<>();
        ArrayList<ArrayList<Tuple<Integer, byte[]>>> data = new ArrayList<>();
        Random random = new Random(System.nanoTime());
        for (int i = 0; i < 10000; i++) {
            int randomItemNum = Math.abs(random.nextInt() % 500) + 1;
            ArrayList<Tuple<Integer, byte[]>> items = new ArrayList<>(randomItemNum);
            for (int j = 0; j < randomItemNum; j++) {
                int keyInt = random.nextInt();
                while (keySet.contains(keyInt)) {
                    keyInt = random.nextInt();
                }
                keySet.add(keyInt);
                byte key[] = intToByteArray(keyInt);
                random.nextBytes(key);
                int randomValueLength = Math.abs(random.nextInt() % 50)+ 1;
                byte value[] = new byte[randomValueLength];
                random.nextBytes(value);
                items.add(new Tuple<Integer, byte[]>(keyInt, value));
            }
            data.add(items);
        }
        logger.info("Start clearning database...");
        // clear database
        ShardedRedisClusterClient.getProcessLevelClient().clearDB();
        // Check single put & get
//        data.parallelStream().forEach(items -> {
//            BasicKVDatabaseClient client = null;
//            try {
//                client = ShardedRedisClusterClient.getProcessLevelClient();
//                for (Tuple<Integer, byte[]> tuple : items) {
//                    client.put(intToByteArray(tuple.x), tuple.y);
////                    byte[] valueGot;
////                    valueGot = client.get(intToByteArray(tuple.x));
////                    if(!Arrays.equals(tuple.y, valueGot)) System.exit(1);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        data.parallelStream().forEach(items -> {
//            BasicKVDatabaseClient client = null;
//            try {
//                client = ShardedRedisClusterClient.getProcessLevelClient();
//                for (Tuple<Integer, byte[]> tuple : items) {
//                    byte valueGot[] = client.get(intToByteArray(tuple.x));
//                    if (!Arrays.equals(valueGot, tuple.y)) {
//                        System.out.println("Different for key:" + tuple.x + "!");
//                        System.out.println("Got:" + Arrays.toString(valueGot));
//                        System.out.println("Expect:" + Arrays.toString(tuple.y));
//                    }
//                    assert Arrays.equals(valueGot, tuple.y);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        // Check batch put & get
//        ShardedRedisClusterClient.getProcessLevelClient().clearDB();
//        data.parallelStream().forEach(items -> {
//            try {
//                BasicKVDatabaseClient client = ShardedRedisClusterClient.getProcessLevelClient();
//                byte keys[][], values[][];
//                keys = new byte[items.size()][];
//                values = new byte[items.size()][];
//                for (int i = 0; i < items.size(); i++) {
//                    keys[i] = intToByteArray(items.get(i).x);
//                    values[i] = items.get(i).y;
//                }
//                client.putAll(keys, values);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        data.parallelStream().forEach(items -> {
//            try {
//                BasicKVDatabaseClient client = ShardedRedisClusterClient.getProcessLevelClient();
//                byte keys[][], values[][];
//                keys = new byte[items.size()][];
//                for (int i = 0; i < items.size(); i++) {
//                    keys[i] = intToByteArray(items.get(i).x);
//                }
//                values = client.getAll(keys);
//                for (int i = 0; i < items.size(); i++) {
//                    assert Arrays.equals(values[i], items.get(i).y);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
        logger.info("Start clearning database...");
        // Check batch put & get with limited batch size
        ShardedRedisClusterClient.getProcessLevelClient().clearDB();
        logger.info("Start batch inserting...");
        data.parallelStream().forEach(items -> {
            try {
                BasicKVDatabaseClient client = ShardedRedisClusterClient.getProcessLevelClient();
                byte keys[][], values[][];
                keys = new byte[items.size()][];
                values = new byte[items.size()][];
                for (int i = 0; i < items.size(); i++) {
                    keys[i] = intToByteArray(items.get(i).x);
                    values[i] = items.get(i).y;
                }
                Utils.batchInput(client, keys, values, 20);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        logger.info("Start batch fetching...");
        data.parallelStream().forEach(items -> {
            try {
                BasicKVDatabaseClient client = ShardedRedisClusterClient.getProcessLevelClient();
                byte keys[][], values[][];
                keys = new byte[items.size()][];
                for (int i = 0; i < items.size(); i++) {
                    keys[i] = intToByteArray(items.get(i).x);
                }
                values = client.getAll(keys);
                for (int i = 0; i < items.size(); i++) {
                    assert Arrays.equals(values[i], items.get(i).y);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

       logger.info("Done!");
    }

    static class Tuple<X, Y> {
        public final X x;
        public final Y y;

        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }
}
