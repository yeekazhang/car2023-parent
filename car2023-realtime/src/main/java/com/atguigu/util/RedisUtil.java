package com.atguigu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private final static JedisPool pool;

    static{
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
        if (jsonStr != null){
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
}
