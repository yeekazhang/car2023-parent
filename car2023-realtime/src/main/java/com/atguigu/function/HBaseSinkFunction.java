package com.atguigu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import com.atguigu.util.HBaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HBaseSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection conn;


    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getHBaseConnection();
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
