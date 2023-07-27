package com.atguigu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T> {

    private StatefulRedisConnection<String, String> redisAsyncConn;
    private AsyncConnection hbaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConn(redisAsyncConn);
        HBaseUtil.closeAsyncHBaseConn(hbaseAsyncConn);
    }

    /**
     * 异步调用, 流中每来一个元素, 这个方法执行一次
     * 参数 1: 需要异步处理的元素
     * 参数 2: 元素被异步处理完之后, 放入到 ResultFuture 中, 则会输出到后面的流中
     *
     * @param bean
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) throws Exception {

        // 1. 从 redis 去维度  第一个异步操作
        // 2. 从 hbase 去读维度 第二个异步操作
        // 3. 把维度补充到 bean 中
        CompletableFuture
                .supplyAsync(new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        return RedisUtil.readDimAsync(redisAsyncConn, getTableName(), getRowKey(bean));
                    }
                })
                .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimFromRedis) {
                        // 当 redis 没有读到数据的时候，从 hbase 中读取数据
                        JSONObject dim = dimFromRedis;
                        if (dim == null) {
                            dim = HBaseUtil.readDimAsync(hbaseAsyncConn, "car", getTableName(), getRowKey(bean));
                            // 维度写到 Redis 中
                            RedisUtil.writeDimAsync(redisAsyncConn, getTableName(), getRowKey(bean), dim);
                        }

                        return dim;
                    }
                })
                .thenAccept(new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dim) {
                        // 补充维度到 bean 中
                        addDims(bean, dim);

                        // 把结果输出
                        resultFuture.complete(Collections.singleton(bean));
                    }
                });

    }
}













