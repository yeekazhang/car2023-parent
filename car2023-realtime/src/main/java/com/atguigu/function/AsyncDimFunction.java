package com.atguigu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
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

public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T,T> implements DimFunction<T>{


    private StatefulRedisConnection<String, String> redisAsyncConn;
    private AsyncConnection hbaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
        hbaseAsyncConn = HBaseUtil.getHbaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
        System.out.println("aaaaaaaaaaaaaaaaaaaaaa");
        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);

    }

    @Override
    public void asyncInvoke(T bean,
                            ResultFuture<T> resultFuture) throws Exception {
        // jdb8 提供的专门用执行异步代码的框架
        // 1. 从 redis 去维度  第一个异步操作
        // 2. 从 hbase 去读维度 第二个异步操作
        // 3. 把维度补充到 bean 中

        CompletableFuture
            .supplyAsync(new Supplier<JSONObject>() {
                @Override
                public JSONObject get() {
                    // 1. 从 redis 去维度  把读到的维度返回
                    return RedisUtil.readDinAsync(redisAsyncConn,getTableName(),getRowKey(bean));


                }
            })
            .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                @Override
                public JSONObject apply(JSONObject dimFromRedis) {
                    // 1. 当Redis中没有读到数据，去Hbase中读
                    JSONObject dim = dimFromRedis;
                    if (dim == null) {
                        dim = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE,getTableName(),getRowKey(bean));

                        //同时把维度数据写到Redis中
                        RedisUtil.writeDimAsync(redisAsyncConn,getTableName(),getRowKey(bean),dim);
                        System.out.println("走的是 hbase " + getTableName() + "  " + getRowKey(bean));
                    }else {
                        System.out.println("走的是 redis " + getTableName() + "  " + getRowKey(bean));
                    }

                    return dim;
                }
            })
            .thenAccept(new Consumer<JSONObject>() {
                @Override
                public void accept(JSONObject dim) {
                    //3.补充维度到bean中
                    addDims(bean,dim);

                    //4.把结果输出
                    resultFuture.complete(Collections.singleton(bean));
                }
            });


    }
}
