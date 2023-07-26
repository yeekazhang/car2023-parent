drop table if exists dws_car_charge_avg;
create table if not exists dws_car_charge_avg
(
    `stt`                          DATETIME comment '窗口起始时间',
    `edt`                          DATETIME comment '窗口结束时间',
    `cur_date`                     DATE comment '当天日期',
    `vin`                 		   VARCHAR(1280) comment '汽车Id',
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
    `vin`                 		   VARCHAR(1280) comment '汽车Id',
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
    `vin`                 		   VARCHAR(1280) comment '汽车Id',
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

