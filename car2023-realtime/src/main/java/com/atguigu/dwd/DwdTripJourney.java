package com.atguigu.dwd;


import com.atguigu.app.BaseSQLApp;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTripJourney extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTripJourney().start(
                30001,
                2,
                "DwdTripJourney"
        );
    }


    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {

        //1.读取ods_db
        readOdsDb(tEnv);

        //2.过滤出行驶数据
        Table carTable = tEnv.sqlQuery(
                "select   " +
                        " vin ," +
                        "`timestamp` ," +
                        "car_status ," +
                        "charge_status ," +
                        "execution_mode ," +
                        "velocity ," +
                        "mileage ," +
                        "voltage ," +
                        "electric_current ," +
                        "soc ," +
                        "dc_status ," +
                        "gear " +
                        "  from ods_log  " +
                        "  where  car_status =  1  "
        );



        tEnv.executeSql("create table  dwd_Running_journey (" +
                "    vin string ," +
                "    `timestamp` bigint," +
                "    car_status  int," +
                "    charge_status int," +
                "    execution_mode int," +
                "    velocity int," +
                "    mileage int," +
                "    voltage int," +
                "    electric_current int," +
                "    soc int," +
                "    dc_status int," +
                "    gear int " +
                " )" + SQLUtil.getKafkaDDLSink("dwd_Running_journey")) ;

        carTable.executeInsert("dwd_Running_journey");






 


    }
}

   
