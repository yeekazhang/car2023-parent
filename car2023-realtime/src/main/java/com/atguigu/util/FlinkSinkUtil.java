package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import com.atguigu.function.HBaseSinkFunction;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;


public class FlinkSinkUtil {
    public static SinkFunction<JSONObject> getHBaseSink() {
        return new HBaseSinkFunction();
    }

    public static DorisSink<String> getDorisSink(String table) {
        Properties properties = new Properties();
        properties.setProperty("format","json");
        properties.setProperty("read_json_by_line","true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.DORIS_FE_NODE)
                        .setTableIdentifier(table)
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build()
                        )
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setLabelPrefix("a")
                        .disable2PC()
                        .setBufferCount(3)
                        .setBufferSize(1024*1024)
                        .setCheckInterval(3000)
                        .setMaxRetries(3)
                        .setStreamLoadProp(properties)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
