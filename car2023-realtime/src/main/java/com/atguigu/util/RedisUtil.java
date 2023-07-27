package com.atguigu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private final static JedisPool pool;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config, Constant.HOST_NAME, 6379);
    }

    public static Jedis getJedis() {
        Jedis jedis = pool.getResource();

        jedis.select(8);

        return jedis;
    }

    public static JSONObject readDim(Jedis jedis, String tableName, String rowKey) {
        String key = getKey(tableName, rowKey);
        String jsonStr = jedis.get(key);
        if (jsonStr != null) {
            return JSON.parseObject(jsonStr);
        }
        return null;
    }

    public static String getKey(String tableName, String rowKey) {
        return tableName + ":" + rowKey;
    }

    public static void writeDim(Jedis jedis, String tableName, String rowKey, JSONObject dim) {
        jedis.setex(getKey(tableName, rowKey), Constant.TWO_DAY_SECONDS, dim.toJSONString());
    }

    /**
     * 获取到 redis 的异步连接
     *
     * @return
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient = RedisClient.create(Constant.REDIS_CLIENT);
        return redisClient.connect();
    }

    public static void closeRedisAsyncConn(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }

    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> redisAsyncConn, String tableName, String rowKey) {
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        String key = getKey(tableName, rowKey);
        try {
            String json = asyncCommand.get(key).get();
            if (json != null) {
                return JSON.parseObject(json);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn, String tableName, String rowKey, JSONObject dim) {
        // 1 得到异步命令
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();

        // 2 写入到 string 中：顺便还设置的 ttl
        String key = getKey(tableName, rowKey);
        asyncCommand.setex(key, Constant.TWO_DAY_SECONDS,  dim.toJSONString());

    }
}
















