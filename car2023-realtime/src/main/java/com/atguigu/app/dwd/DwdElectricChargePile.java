package com.atguigu.app.dwd;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 * 充电桩充电车辆电池电压状态
 *  1. 车辆状态必须是停下充电
 *      car_status = 2
 *  2. 充电状态是充电桩状态
 *      charge_status = 1 or 4
 *
 */
public class DwdElectricChargePile extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdElectricChargePile().start(
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
        Table result = tEnv.sqlQuery(
                "select " +
                        "   vin, " +
                        "   `timestamp`, " +
                        "   max_voltage_battery_pack_id, " +
                        "   max_voltage_battery_id, " +
                        "   max_voltage, " +
                        "   min_temperature_subsystem_id, " +
                        "   min_voltage_battery_id, " +
                        "   min_voltage " +
                        "from ods_log " +
                        "where car_status=2 " +
                        "and (charge_status=1 or charge_status=4) "
        );

        // 3 写出到 kafka 中
        tEnv.executeSql("create table dwd_electric_charging_pile( " +
                "   vin string, " +
                "   `timestamp` bigint, " +
                "   max_voltage_battery_pack_id int, " +
                "   max_voltage_battery_id int, " +
                "   max_voltage int, " +
                "   min_temperature_subsystem_id int, " +
                "   min_voltage_battery_id int, " +
                "   min_voltage int " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_ELECTRIC_CHARGE_PILE));

        result.executeInsert("dwd_electric_charging_pile");

    }
}


/**
 *
 * 充电桩充电车辆电池电压状态
 *  1. 车辆状态必须是停下充电
 *      car_status = 2
 *  2. 充电状态是充电桩状态
 *      charge_status = 1 or 4
 *
 */