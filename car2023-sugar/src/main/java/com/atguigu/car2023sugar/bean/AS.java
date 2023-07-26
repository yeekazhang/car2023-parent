package com.atguigu.car2023sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AS {
    private Integer alert_level;
    private BigDecimal alert_ct;
}
