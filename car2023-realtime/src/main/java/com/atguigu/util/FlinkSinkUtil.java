package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.function.HBaseSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class FlinkSinkUtil {
    public static SinkFunction<JSONObject> getHBaseSink() {
        return new HBaseSinkFunction();
    }
}
