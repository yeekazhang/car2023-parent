package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.common.Constant;
import com.atguigu.util.FlinkSinkUtil;
import com.atguigu.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(20001,
            2,
            "DimApp",
            Constant.TOPIC_ODS_LOG
           );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1. 通过 flink cdc 读取维度表的数据
        Map<String, DataStream<JSONObject>> streams = readFromDim(env);
        //streams.get("dicCode").print();

        //2. HBase 中建表
        try {
            createHBaseTable();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //3. 写到Hbase中
        writeToHbase(streams);




    }

    private void writeToHbase(Map<String, DataStream<JSONObject>> streams) {
        streams
            .get("carInfo")
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject map(JSONObject value) throws Exception {
                    String op = value.getString("op");
                    JSONObject obj = new JSONObject();
                    JSONObject data;
                    if ("d".equals(op)) {
                        data = value.getJSONObject("before");
                    }else {
                        data = value.getJSONObject("after");
                    }
                    obj.put("data", data);
                    obj.put("op", op);
                    obj.put("rowKey",data.getString("id"));
                    String table = "dim_" + value.getJSONObject("source").getString("table");
                    obj.put("sinkTable", table);
                    return obj;
                }
            })
            .addSink(FlinkSinkUtil.getHBaseSink());


        streams
            .get("dicCode")
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject map(JSONObject value) throws Exception {
                    String op = value.getString("op");
                    JSONObject obj = new JSONObject();
                    JSONObject data;
                    if ("d".equals(op)) {
                        data = value.getJSONObject("before");
                    }else {
                        data = value.getJSONObject("after");
                    }
                    obj.put("data", data);
                    obj.put("op", op);
                    obj.put("rowKey",data.getString("dic_id"));
                    String table = "dim_" + value.getJSONObject("source").getString("table");
                    obj.put("sinkTable", table);
                    return obj;
                }
            })
            .addSink(FlinkSinkUtil.getHBaseSink());


    }

    private void createHBaseTable() throws IOException {
        // 1. 获取到 HBase 的连接
        Connection hbaseConn = HBaseUtil.getHBaseConnection();
        // 2. 创建car_info表
        HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE, "dim_car_info", "info");
        // 3. 创建字典表
        HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE, "dim_car_date_code", "info");

        // 5. 关闭连接
        HBaseUtil.closeHBaseConn(hbaseConn);
    }

    private Map<String, DataStream<JSONObject>> readFromDim(StreamExecutionEnvironment env) {

        OutputTag<JSONObject> dimCodeTag = new OutputTag<JSONObject>("code"){};

        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname(Constant.MYSQL_HOST)
            .port(Constant.MYSQL_PORT)
            .databaseList("car_data")
            .tableList("car_data.car_info, car_data.car_date_code")
            .username(Constant.MYSQL_USER_NAME)
            .password(Constant.MYSQL_PASSWORD)
            .jdbcProperties(props)
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .startupOptions(StartupOptions.initial()) // 默认值: initial  第一次启动读取所有数据(快照), 然后通过 binlog 实时监控变化数据
            .build();

        SingleOutputStreamOperator<JSONObject> carInfoStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
            .setParallelism(1)
            .map(JSON::parseObject)
            .process(new ProcessFunction<JSONObject, JSONObject>() {
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    String table = obj.getJSONObject("source").getString("table");
                    if ("car_info".equals(table)) {
                        out.collect(obj);
                    }
                    if ("car_date_code".equals(table)) {
                        ctx.output(dimCodeTag, obj);
                    }
                }
            });

        Map<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put("carInfo", carInfoStream);
        streams.put("dicCode", carInfoStream.getSideOutput(dimCodeTag));

        return streams;
    }
}
