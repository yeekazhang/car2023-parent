package com.atguigu.car2023sugar.service;

import com.atguigu.car2023sugar.bean.AS;

import java.math.BigDecimal;
import java.util.List;

public interface AlertService {
    List<AS> alertSumByLevel(String date);
}
