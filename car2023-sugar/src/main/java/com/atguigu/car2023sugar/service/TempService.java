package com.atguigu.car2023sugar.service;

import com.atguigu.car2023sugar.bean.AS;
import com.atguigu.car2023sugar.bean.MotorTemp;

import java.util.List;

public interface TempService {
    List<MotorTemp> sumAndAvgOfMotorTemp(String date);
}
