package com.atguigu.app.dwd;

import com.atguigu.app.BaseApp;
import com.atguigu.app.BaseSQLApp;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTempMotor extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTempMotor().start(
                30010,
                 2,
                 "DwdTempMotor"
        );
    }


    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {

        readOdsLog(tEnv);

        Table result = tEnv.sqlQuery("select " +
                " `vin`  ," +
                " `timestamp`  ," +
                " motor_count  ," +
                " motor_list " +
                "from  ods_log ");
        tEnv.executeSql("create table dwd_temp_motor (" +
                " `vin` string  ," +
                " `timestamp`  ," +
                " motor_count  ," +
                " motor_list " +
                "" + SQLUtil.getKafkaDDLSink("dwd_temp_motor"));



    }
}
