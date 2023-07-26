package com.atguigu.app.dws;

import com.atguigu.app.BaseApp;
import com.atguigu.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsTempMotorTemperatureControlWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTempMotorTemperatureControlWindow().start(
                40004,
                2,
                "DwsTempMotorTemperatureControlWindow",
                Constant.TOPIC_DWD_TEMP_MOTOR
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

    }
}
