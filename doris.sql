drop table if exists dws_car_charge_avg;
create table if not exists dws_car_charge_avg
(
    `stt`                          DATETIME comment '窗口起始时间',
    `edt`                          DATETIME comment '窗口结束时间',
    `cur_date`                     DATE comment '当天日期',
    `vin`                 		   VARCHAR(20) comment '汽车Id',
    `total_vol`    					BIGINT  replace comment '平均电压分子',
    `total_electric_current` 		BIGINT  replace comment '平均电流分子',
    `total_insulation_resistance`   BIGINT  replace comment '平均绝缘电阻分子',
    `num`                 			BIGINT  replace comment '平均分母'
) engine = olap
    aggregate key (
`stt`,`edt`,`cur_date`,`vin`
)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
  "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10"
);



drop table if exists dws_car_discharge_avg;
create table if not exists dws_car_discharge_avg
(
    `stt`                          DATETIME comment '窗口起始时间',
    `edt`                          DATETIME comment '窗口结束时间',
    `cur_date`                     DATE comment '当天日期',
    `vin`                 		   VARCHAR(20) comment '汽车Id',
    `total_vol`    					BIGINT  replace comment '平均电压分子',
    `total_electric_current` 		BIGINT  replace comment '平均电流分子',
    `total_insulation_resistance`   BIGINT  replace comment '平均绝缘电阻分子',
    `num`                 			BIGINT  replace comment '平均分母'
) engine = olap
    aggregate key (
`stt`,`edt`,`cur_date`,`vin`
)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
  "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10"
);

drop table if exists dws_car_trip_count;
create table if not exists dws_car_trip_count
(
    `stt`                          DATETIME comment '窗口起始时间',
    `edt`                          DATETIME comment '窗口结束时间',
    `cur_date`                     DATE comment '当天日期',
    `vin`                 		   VARCHAR(20) comment '汽车Id',
    `mileage`    					BIGINT  replace comment '里程表总里程',
    `one_count` 					BIGINT  replace comment '距离上次数据行驶里程'
) engine = olap
    aggregate key (
`stt`,`edt`,`cur_date`,`vin`
)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
  "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10"
);

create database car;
use car;

drop table if exists dws_alert_alert_count;
create table if not exists dws_alert_alert_count
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `vin`           varchar(20) comment '汽车id',
    `alert_level`   varchar(5) comment '告警等级',
    `alert_ct`      bigint replace comment '告警次数'

) engine = olap
aggregate key (`stt`, `edt`, `cur_date`, `vin`, `alert_level`)
comment "告警域-车辆告警等级粒度告警次数汇总表"
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
    "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10"
);

drop table if exists dws_temp_battery_temperature_control;
create table if not exists dws_temp_battery_temperature_control
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `vin`           varchar(20) comment '汽车id',
    `max_temperature`   int replace comment '电池最高温度',
    `battery_abnormal_ct`      bigint replace comment '电池温度异常值次数'

) engine = olap
    aggregate key (`stt`, `edt`, `cur_date`, `vin`)
comment "温度域-汽车电池粒度温度汇总表"
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
    "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10"
);

drop table if exists  dws_temp_motor_temperature_control;
create table if not exists dws_temp_motor_temperature_control
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `vin`           varchar(20) comment '汽车id',
    `motor_max_temperature`   int replace comment '电机最高温度',
    `control_max_temperature` int replace comment '控制器最高温度',
    `motor_acc_temperature` bigint replace comment '电机平均温度',
    `control_acc_temperature` bigint replace comment '控制器平均温度',
    `motor_acc_ct` bigint replace comment '电机累计数量'
) engine = olap
    aggregate key (`stt`, `edt`, `cur_date`, `vin`)
comment "温度域-汽车粒度电机和控制器温度汇总表"
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
    "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10"
);

drop table if exists dws_temp_battery_temperature_control;
create table if not exists dws_temp_battery_temperature_control
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期',
    `vin`           varchar(20) comment '汽车id',
    `battery_max_voltage`   int replace comment '电池最大电压',
    `battery_max_diff_voltage`  int replace comment '电池最大电压差',
    `battery_voltage_warn_times`  int replace comment '电池电压警告次数'

) engine = olap
    aggregate key (`stt`, `edt`, `cur_date`, `vin`)
comment "电控域电池电压数据汇总表"
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
    "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10"
);


drop table if exists dws_electric_accelerator_agg_electric_window;
create table if not exists dws_electric_accelerator_agg_electric_window
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期' ,
    `vin`  VARCHAR(30)  comment '	汽车唯一ID',
    `sum_electric`  DOUBLE  replace comment '电机总电流',
    `electric_count` int replace  comment '统计次数'

    )
    engine = olap
    aggregate key (`stt`, `edt`,`cur_date`,`vin`)
    comment '电压域各车油门电流累计表'
    partition by range(`cur_date`)()
    distributed by hash(`stt`) buckets 10
    properties (
                   "replication_num" = "1",
                   "dynamic_partition.enable" = "true",
                   "dynamic_partition.time_unit" = "DAY",
                   "dynamic_partition.end" = "3",
                   "dynamic_partition.prefix" = "par",
                   "dynamic_partition.buckets" = "10"
               );



drop table if exists dws_energy_charge_cycles_window;
create table if not exists dws_energy_charge_cycles_window
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `cur_date`      DATE comment '当天日期' ,
    `vin`  VARCHAR(30)  comment '	汽车唯一ID',
    `charge_cycles`  int  replace comment '总充电次数',
    `charge_slow_cycles`  int  replace comment '总慢充电次数',
    `charge_fast_cycles`  int  replace comment '总快充电次数'
    )
    engine = olap
    aggregate key (`stt`, `edt`,`cur_date`,`vin`)
    comment '电压域各车充电次数汇总表'
    partition by range(`cur_date`)()
    distributed by hash(`stt`) buckets 10
    properties (
                   "replication_num" = "1",
                   "dynamic_partition.enable" = "true",
                   "dynamic_partition.time_unit" = "DAY",
                   "dynamic_partition.end" = "3",
                   "dynamic_partition.prefix" = "par",
                   "dynamic_partition.buckets" = "10"
               );