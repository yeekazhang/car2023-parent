package com.atguigu.car2023sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MotorTemp {
    private String power_type;
    private Integer motor_max_temperature;
    private Integer control_max_temperature;
    private BigDecimal motor_avg_temperature;
    private BigDecimal control_avg_temperature;
}
