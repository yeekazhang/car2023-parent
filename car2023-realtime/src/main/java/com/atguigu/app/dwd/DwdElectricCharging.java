package com.atguigu.app.dwd;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 车辆充电状态
 *  charge_status = 1, 2, 4
 */
public class DwdElectricCharging extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdElectricCharging().start(
                10011,
                2,
                "DwdElectricCharging"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        readOdsLog(tEnv);

        Table result = tEnv.sqlQuery(
                "select " +
                        "   vin, " +
                        "   `timestamp`, " +
                        "   voltage, " +
                        "   electric_current, " +
                        "   insulation_resistance " +
                        "from ods_log " +
                        "where charge_status = 1 " +
                        "or charge_status = 2 " +
                        "or charge_status = 4"
        );

        tEnv.executeSql("create table dwd_electric_charging(" +
                "   vin string, " +
                "   `timestamp` bigint, " +
                "   voltage int, " +
                "   electric_current int, " +
                "   insulation_resistance bigint" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_ELECTRIC_CHARGING));

        result.executeInsert("dwd_electric_charging");

    }
}

