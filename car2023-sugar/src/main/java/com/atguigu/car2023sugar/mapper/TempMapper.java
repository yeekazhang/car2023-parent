package com.atguigu.car2023sugar.mapper;

import com.atguigu.car2023sugar.bean.MotorTemp;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TempMapper {
    @Select("select " +
            "    power_type, " +
            "    max(motor_max_temperature) motor_max_temperature, " +
            "    max(control_max_temperature) control_max_temperature, " +
            "    sum(control_acc_temperature) / sum(motor_acc_ct) motor_avg_temperature, " +
            "    sum(control_acc_temperature) / sum(motor_acc_ct) control_avg_temperature " +
            "from dws_temp_motor_temperature_control partition(par${date}) " +
            "group by power_type;")
    List<MotorTemp> sumAndAvgOfMotorTemp(String date);
}
