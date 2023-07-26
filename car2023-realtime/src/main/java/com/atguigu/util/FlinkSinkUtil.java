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
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
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
                .disable2PC()  // 禁用两阶段提交: 如果不禁用LabelPrefix必须全局唯一
                .setBufferCount(3)
                .setBufferSize(1024 * 1024)
                .setCheckInterval(3000)
                .setMaxRetries(3)
                .setStreamLoadProp(props)
                .build())
            .setSerializer(new SimpleStringSerializer())
            .build();

    }
}
