package com.atguigu.app.dwd;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 车辆放电状态
 *  车辆在运行中并且运行模式不是燃料模式
 *      car_status=1 and execution_mode!=3
 *  并且充电状态是不在充电
 *      and charge_status=3
 */
public class DwdElectricDischarging extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdElectricDischarging().start(
                10012,
                2,
                "DwdElectricDischarging"
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
                        "where car_status = 1 " +
                        "and execution_mode <> 3 " +
                        "and charge_status = 3"
        );

        tEnv.executeSql("create table dwd_electric_discharging(" +
                "   vin string, " +
                "   `timestamp` bigint, " +
                "   voltage int, " +
                "   electric_current int, " +
                "   insulation_resistance bigint" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_ELECTRIC_DISCHARGING));

        result.executeInsert("dwd_electric_discharging");

    }
}

