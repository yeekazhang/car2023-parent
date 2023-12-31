package com.atguigu.app.dwd;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdAlertWarn extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdAlertWarn().start(
                30002,
                2,
                "DwdAlertWarn"
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {

        //1.读取ods_db
        readOdsLog(tEnv);

        //2.过滤出告警数据
        Table carTable = tEnv.sqlQuery(
                "select   " +
                        " `vin`  ," +
                        " `timestamp`  ," +
                        " `car_status`  ," +
                        " `charge_status`  ," +
                        " `execution_mode`  ," +
                        " `velocity`   ," +
                        " `mileage`  ," +
                        " `voltage`  ," +
                        " `electric_current`  ," +
                        " `soc`  ," +
                        " `dc_status`  ," +
                        "  gear  ," +
                        "  insulation_resistance  ," +
                        "  motor_count  ," +
                        "  motor_list  ," +
                        "  fuel_cell_voltage   ," +
                        "  fuel_cell_current   ," +
                        "  fuel_cell_consume_rate   ," +
                        "  fuel_cell_temperature_probe_count   ," +
                        "  fuel_cell_temperature   ," +
                        "  fuel_cell_max_temperature   ," +
                        "  fuel_cell_max_temperature_probe_id   ," +
                        "  fuel_cell_max_hydrogen_consistency   ," +
                        "  fuel_cell_max_hydrogen_consistency_probe_id   ," +
                        "  fuel_cell_max_hydrogen_pressure   ," +
                        "  fuel_cell_max_hydrogen_pressure_probe_id   ," +
                        "  fuel_cell_dc_status   ," +
                        "  engine_status   ," +
                        "  crankshaft_speed   ," +
                        "  fuel_consume_rate   ," +
                        "  alarm_level   ," +
                        "  alarm_sign   ," +
                        "  custom_battery_alarm_count   , " +
                        "  custom_battery_alarm_list  ," +
                        "  custom_motor_alarm_count  ," +
                        "  custom_motor_alarm_list ," +
                        "  custom_engine_alarm_count  ," +
                        "  custom_engine_alarm_list ," +
                        "  other_alarm_count   ," +
                        "  other_alarm_list  ," +
                        "  battery_count   ," +
                        "  battery_pack_count   ," +
                        "  battery_voltages  ," +
                        "  battery_temperature_probe_count   ," +
                        "  battery_pack_temperature_count   ," +
                        "  battery_temperatures  ," +
                        "  max_voltage_battery_pack_id   ," +
                        "  max_voltage_battery_id   ," +
                        "  max_voltage   ," +
                        "  min_voltage_battery_pack_id   ," +
                        "  min_voltage_battery_id   ," +
                        "  min_voltage   ," +
                        "  max_temperature_subsystem_id   ," +
                        "  max_temperature_probe_id   ," +
                        "  max_temperature   ," +
                        "  min_temperature_subsystem_id   ," +
                        "  min_temperature_probe_id   ," +
                        "  min_temperature     " +
                        "  from ods_log  " +
                        "  where  alarm_level >  0  "
        );

        //写出到kafka
        tEnv.executeSql("create table  dwd_alert_warn ( " +
                "         `vin`  string ," +
                "         `timestamp`  bigint ," +
                "         `car_status`  int ," +
                "         `charge_status`  int ," +
                "         `execution_mode`  int ," +
                "         `velocity`   int ," +
                "         `mileage`  int ," +
                "         `voltage`  int ," +
                "         `electric_current`  int ," +
                "         `soc`  int ," +
                "         `dc_status`  int ," +
                "         gear  int ," +
                "         insulation_resistance  int ," +
                "         motor_count  int ," +
                "         motor_list  ARRAY<ROW<id INT, status INT, controller_temperature INT, rev INT, torque INT, temperature INT, voltage INT, electric_current INT>>  ," +
                "         fuel_cell_voltage   int ," +
                "         fuel_cell_current   int ," +
                "         fuel_cell_consume_rate   int ," +
                "         fuel_cell_temperature_probe_count   int ," +
                "         fuel_cell_temperature   int ," +
                "         fuel_cell_max_temperature   int ," +
                "         fuel_cell_max_temperature_probe_id   int ," +
                "         fuel_cell_max_hydrogen_consistency   int ," +
                "         fuel_cell_max_hydrogen_consistency_probe_id   int ," +
                "         fuel_cell_max_hydrogen_pressure   int ," +
                "         fuel_cell_max_hydrogen_pressure_probe_id   int ," +
                "         fuel_cell_dc_status   int ," +
                "         engine_status   int ," +
                "         crankshaft_speed   int ," +
                "         fuel_consume_rate   int ," +
                "         alarm_level   int ," +
                "         alarm_sign   int ," +
                "         custom_battery_alarm_count   int ," +
                "         custom_battery_alarm_list  array<int>  ," +
                "         custom_motor_alarm_count  int ," +
                "         custom_motor_alarm_list array<int>  ," +
                "         custom_engine_alarm_count  int ," +
                "         custom_engine_alarm_list array<int>  ," +
                "         other_alarm_count   int ," +
                "         other_alarm_list  array<int> ," +
                "         battery_count   int ," +
                "         battery_pack_count   int ," +
                "         battery_voltages  array<int> ," +
                "         battery_temperature_probe_count   int ," +
                "         battery_pack_temperature_count   int ," +
                "         battery_temperatures  array<int> ," +
                "         max_voltage_battery_pack_id   int ," +
                "         max_voltage_battery_id   int ," +
                "         max_voltage   int ," +
                "         min_voltage_battery_pack_id   int ," +
                "         min_voltage_battery_id   int ," +
                "         min_voltage   int ," +
                "         max_temperature_subsystem_id   int ," +
                "         max_temperature_probe_id   int ," +
                "         max_temperature   int ," +
                "         min_temperature_subsystem_id   int ," +
                "         min_temperature_probe_id   int ," +
                "         min_temperature    int  " +
                " )" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_ALERT_WARN)) ;

        carTable.executeInsert("dwd_alert_warn");




    }
}

