package com.atguigu.app.dwd;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdEnergyCharge extends BaseSQLApp {
    public static void main(String[] args) {

        new DwdEnergyCharge().start(
                30009,
                2,
                "DwdEnergyCharge"
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        //1.读取ods_log

        readOdsLog(tEnv);

        //2.过滤出充电的数据

        Table result = tEnv.sqlQuery("select  " +
                " `vin`  ," +
                " `timestamp`  ," +
                " `car_status`  ," +
                " `charge_status`  ," +
                " `execution_mode` ," +
                " `velocity`   ," +
                " `mileage`  ," +
                " `voltage`  ," +
                " `electric_current`  ," +
                " `soc`  ," +
                " `dc_status`  ," +
                "  gear  ," +
                "  insulation_resistance " +
                "  from  ods_log " +
                "   where  charge_status in (1,2,3)");


        tEnv.executeSql("create table dwd_energy_charge ( " +
                " `vin` string ," +
                " `timestamp` bigint ," +
                " `car_status` int ," +
                " `charge_status` int ," +
                " `execution_mode` int ," +
                " `velocity`  int ," +
                " `mileage` int ," +
                " `voltage` int ," +
                " `electric_current` int ," +
                " `soc` int ," +
                " `dc_status` int ," +
                "  gear int ," +
                "  insulation_resistance int " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_ENERGY_CHARGE));


        //3.写到kafka
        result.executeInsert("dwd_energy_charge");




    }
}
