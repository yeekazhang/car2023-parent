package com.atguigu.app;

import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {
    protected abstract void handle(StreamExecutionEnvironment env,
                                   StreamTableEnvironment tEnv);

    public void  start(int port ,int p, String ck ){
        System.setProperty("HADOOP_USER_NAME","atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
       //1.设置状态后端
        env.setStateBackend(new HashMapStateBackend());
       //2.开启checkpoint
       env.enableCheckpointing(3000);
       //3.设置checkpoint模式 语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//
       //4.checkpoint存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/car2023/"+ck);
       //5.设置checkpoint最大并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //6. checkpoint的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //7.check的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //8.job取消的时候的checkpoint的保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        handle(env,tEnv);



    }

   public static  void readOdsLog( StreamTableEnvironment tEnv){

        tEnv.executeSql("create table ods_log ( " +
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
                "  insulation_resistance int ," +
                "  motor_count int ," +
                "  motor_list  ARRAY<ROW<id INT, status INT, controller_temperature INT, rev INT, torque INT, temperature INT, voltage INT, electric_current INT>> ," +
                "  fuel_cell_voltage  int ," +
                "  fuel_cell_current  int ," +
                "  fuel_cell_consume_rate  int ," +
                "  fuel_cell_temperature_probe_count  int ," +
                "  fuel_cell_temperature  int ," +
                "  fuel_cell_max_temperature  int ," +
                "  fuel_cell_max_temperature_probe_id  int ," +
                "  fuel_cell_max_hydrogen_consistency  int ," +
                "  fuel_cell_max_hydrogen_consistency_probe_id  int ," +
                "  fuel_cell_max_hydrogen_pressure  int ," +
                "  fuel_cell_max_hydrogen_pressure_probe_id  int ," +
                "  fuel_cell_dc_status  int ," +
                "  engine_status  int ," +
                "  crankshaft_speed  int ," +
                "  fuel_consume_rate  int ," +
                "  alarm_level  int ," +
                "  alarm_sign  int ," +
                "  custom_battery_alarm_count  int , " +
                "  custom_battery_alarm_list  array<int> ," +
                "  custom_motor_alarm_count int ," +
                "  custom_motor_alarm_list array<int> ," +
                "  custom_engine_alarm_count int ," +
                "  custom_engine_alarm_list array<int> ," +
                "  other_alarm_count  int ," +
                "  other_alarm_list  array<int> ," +
                "  battery_count  int ," +
                "  battery_pack_count  int ," +
                "  battery_voltages  array<int> ," +
                "  battery_temperature_probe_count  int ," +
                "  battery_pack_temperature_count  int ," +
                "  battery_temperatures  array<int> ," +
                "  max_voltage_battery_pack_id  int ," +
                "  max_voltage_battery_id  int ," +
                "  max_voltage  int ," +
                "  min_voltage_battery_pack_id  int ," +
                "  min_voltage_battery_id  int ," +
                "  min_voltage  int ," +
                "  max_temperature_subsystem_id  int ," +
                "  max_temperature_probe_id  int ," +
                "  max_temperature  int ," +
                "  min_temperature_subsystem_id  int ," +
                "  min_temperature_probe_id  int ," +
                "  min_temperature  int  " +
                " )"+ SQLUtil.getKafkaSource(Constant.TOPIC_ODS_LOG,"kafka-source"));
   }

    public static  void readBaseDic(StreamTableEnvironment tEnv){

        tEnv.executeSql(
                "create table base_dic (" +
                " dic_code string," + //如果是原子类型，则表示这个是rowkey，字段类型和名字随意
                " info row<dic_name string>," +  //字段名必须是hbase中的列族保持一致，类型必须是row，嵌套进去的就是列
                " primary key (dic_code) not enforced" +
                ") with (" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name' = 'gmall:dim_base_dic'," +
                " 'zookeeper.quorum' = 'hadoop162:2181'," +
                " 'lookup.async' = 'true'," +
                " 'lookup.cache' = 'PARTIAL'," +
                " 'lookup.partial-cache.max-rows' = '20'," +
                " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                ");");



    }





}
