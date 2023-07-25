package com.atguigu.app.dwd;

import com.atguigu.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdElectChargingPile extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdElectChargingPile().start(
                30010,
                2,
                "DwdElectChargingPile"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1 读取 ods_db 数据
        readOdsLog(tEnv);

        // 2 过滤出充电桩充电数据


        // 3 写出到 kafka 中

    }
}
