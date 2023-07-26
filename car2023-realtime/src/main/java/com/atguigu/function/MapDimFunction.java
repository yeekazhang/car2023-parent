package com.atguigu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public abstract class MapDimFunction<T> extends RichMapFunction<T,T> implements DimFunction<T>{

    private Connection hBaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConn = HBaseUtil.getHBaseConnection();

        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hBaseConn);

        RedisUtil.closeJedis(jedis);
    }





    @Override
    public T map(T bean) throws Exception {

        //1.先去Redis读取维度
        JSONObject dim = RedisUtil.readDim(jedis, getTableName(), getRowKey(bean));
        //2.如果读到则直接返回

        //3.如果没有读到，则去Hbase读，并把读到的维度缓存到Redis中
        if (dim == null) {
            dim = HBaseUtil.getRow(hBaseConn,
                "car",
                getTableName(),
                getRowKey(bean),
                JSONObject.class);

            //4.把独到的维度数据缓存到Redis中
            RedisUtil.writeDim(jedis, getTableName(), getRowKey(bean),dim);

            System.out.println("走 hbase: " + getTableName() + "  " + getRowKey(bean));
        }else {
            System.out.println("走 redis: " + getTableName() + "  " + getRowKey(bean));
        }

        //补充维度
        addDims(bean,dim);

        return bean;
    }
}
