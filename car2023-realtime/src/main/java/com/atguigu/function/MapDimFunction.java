package com.atguigu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public abstract class MapDimFunction<T> extends RichMapFunction<T, T> implements DimFunction<T> {

    private Connection hbaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();

        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hbaseConn);

    }

    @Override
    public T map(T bean) throws Exception {

        // 1 先取 redis 读取维度
        JSONObject dim = RedisUtil.readDim(jedis, getTableName(), getRowKey(bean));

        if(dim == null){
            // 2 如果没有读到,则去 hbase 读取
            dim = HBaseUtil.getRow(hbaseConn, "car", getTableName(), getRowKey(bean), JSONObject.class);

            // 3 并把读到的维度缓存到redis中
            RedisUtil.writeDim(jedis, getTableName(), getRowKey(bean), dim);
            System.out.println("走 hbase: " + getTableName() + "  " + getRowKey(bean));
        } else {
            System.out.println("走 redis: " + getTableName() + "  " + getRowKey(bean));
        }

        // 补充维度
        addDims(bean, dim);
        return bean;
    }
}
