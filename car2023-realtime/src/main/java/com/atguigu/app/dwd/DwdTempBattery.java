package com.atguigu.app.dwd;

import com.atguigu.app.BaseApp;
import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTempBattery extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTempBattery().start(
                30012,
                2,
                "DwdTempBattery"
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        readOdsLog(tEnv);

        Table result = tEnv.sqlQuery(" select  " +
                        "`vin`  ," +
                        "`timestamp`  ," +
                        "`car_status`  ," +
                        "`charge_status`  ," +
                        "`execution_mode`  ," +
                        "`velocity`   ," +
                        "`mileage`  ," +
                        "`voltage`  ," +
                        "`electric_current`  ," +
                        "`soc`  ," +
                        "`dc_status`  ," +
                        " gear  ," +
                        "max_temperature_subsystem_id   ," +
                        "max_temperature_probe_id   ," +
                        "max_temperature   ," +
                        "min_temperature_subsystem_id   ," +
                        "min_temperature_probe_id   ," +
                        "min_temperature " +
                "from ods_log  ");

        tEnv.executeSql("create table dwd_temp_battery (" +
                "`vin` string ," +
                "`timestamp` bigint ," +
                "`car_status` int ," +
                "`charge_status` int ," +
                "`execution_mode` int ," +
                "`velocity`  int ," +
                "`mileage` int ," +
                "`voltage` int ," +
                "`electric_current` int ," +
                "`soc` int ," +
                "`dc_status` int ," +
                " gear int ," +
                "max_temperature_subsystem_id  int ," +
                "max_temperature_probe_id  int ," +
                "max_temperature  int ," +
                "min_temperature_subsystem_id  int ," +
                "min_temperature_probe_id  int ," +
                "min_temperature  int  " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.topic_dwd_temp_battery));

        result.executeInsert("dwd_temp_battery");


    }
}
