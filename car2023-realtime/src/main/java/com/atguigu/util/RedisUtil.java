package com.atguigu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutionException;

public class RedisUtil {

    private static final JedisPool pool;

    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config,"hadoop162",6379);
    }


    public static Jedis getJedis(){

        Jedis jedis = pool.getResource();
        jedis.select(5);
        return jedis;
    }

    /**
     * 获取到Redis的异步连接
     * @return
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient = RedisClient.create("redis://hadoop162:6379/5");
        return redisClient.connect();
    }



    /**
     * 关闭 redis 的异步连接
     *
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

    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        String jsonStr = jedis.get(key);
        if (jsonStr != null) {
            return JSONObject.parseObject(jsonStr);
        }
        return null;
    }

    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dim) {

       /* jedis.set(getKey(tableName,id),dim.toJSONString());
        jedis.expire(getKey(tableName,id), Constant.TWO_DAY_SECONDS);*/

        jedis.setex(getKey(tableName, id), Constant.TWO_DAY_SECONDS, dim.toJSONString());

    }

    public static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    public static JSONObject readDinAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                          String tableName,
                                          String id) {
        // 1. 得到异步命令
        RedisAsyncCommands<String, String> asyncCommon = redisAsyncConn.async();
        String key = getKey(tableName, id);
        try {
            String json = asyncCommon.get(key).get();

            if (json != null) {
                return JSON.parseObject(json);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }


    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                     String tableName,
                                     String id,
                                     JSONObject dim) {
        // 1. 得到异步命令
        RedisAsyncCommands<String, String> asyncCommon = redisAsyncConn.async();

        String key = getKey(tableName, id);
        // 2. 写入到 string 中: 顺便还设置的 ttl
        asyncCommon.setex(key, Constant.TWO_DAY_SECONDS, dim.toJSONString());
    }
}
