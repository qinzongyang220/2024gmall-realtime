package com.bw.utils;

import com.bw.common.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class RedisUtil {
    private final static JedisPool pool;

    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
//        config.setMaxTotal(300);
//        config.setMaxIdle(10);
//        config.setMinIdle(2);
        config.setMaxTotal(500);
        config.setMaxIdle(100);
        config.setMinIdle(20);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
//        config.setTestOnBorrow(false);
        config.setTestOnReturn(true);

//        config.setTimeBetweenEvictionRunsMillis(60000);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config, "hadoop102", 6379);
    }

    public static Jedis getJedis() {
        // Jedis jedis = new Jedis("hadoop102", 6379);

        Jedis jedis = pool.getResource();
        jedis.select(1); // 直接选择 4 号库

        return jedis;
    }

    /**
     * 获取异步链接
     * @return
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/2");
        return redisClient.connect();
    }

    /**
     * 关闭异步链接
     * @param redisAsyncConn
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }

    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();  // 如果 jedis 客户端是 new Jedis()得到的,则是关闭客户端.如果是通过连接池得到的,则归还
        }
    }

    public static String getRedisKey(String tableName,String id){
        return Constant.HBASE_NAMESPACE + ":" + tableName + ":" + id;
    }

}
