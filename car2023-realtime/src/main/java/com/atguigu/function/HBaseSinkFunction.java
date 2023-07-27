package com.atguigu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import com.atguigu.util.HBaseUtil;
import com.atguigu.util.RedisUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class HBaseSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection conn;
    private Jedis jedis;


    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(conn);
    }

    @Override
    public void invoke(JSONObject obj, Context context) throws Exception {

        String opType = obj.getString("op");

        if ("d".equals(opType)) {
            // 删除维度信息
            delDim(obj);
        }else{
            // insert 和 update 的时候, 写入维度数据
            putDim(obj);
        }

        //如果维度是update或delete 则删除缓存中的维度数据
        if ("d".equals(opType) || "u".equals(opType)) {
            String key = RedisUtil.getKey(obj.getString("sinkTable"), obj.getString("rowKey"));
            jedis.del(key);
            System.out.println("维度数据发生变化，Redis 中已经删除了" + key + "的维度数据");
        }

    }

    private void putDim(JSONObject obj) throws IOException {
        JSONObject data = obj.getJSONObject("data");

        HBaseUtil.putRow(conn,
            Constant.HBASE_NAMESPACE,
            obj.getString("sinkTable"),
            obj.getString("rowKey"),
            "info",
            data);
    }

    private void delDim(JSONObject obj) throws IOException {
        JSONObject data = obj.getJSONObject("data");
        HBaseUtil.delRow(conn,
            Constant.HBASE_NAMESPACE,
            obj.getString("sinkTable"),
            obj.getString("rowKey"));
    }
}
