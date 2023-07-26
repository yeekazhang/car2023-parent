package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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
}
