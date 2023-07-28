package com.atguigu.car2023sugar.service;

import com.atguigu.car2023sugar.bean.AS;
import com.atguigu.car2023sugar.bean.MotorTemp;
import com.atguigu.car2023sugar.mapper.AlertMapper;
import com.atguigu.car2023sugar.mapper.TempMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TempServiceImpl implements TempService{

    @Autowired
    TempMapper tempMapper;

    @Override
    public List<MotorTemp> sumAndAvgOfMotorTemp(String date) {
        return tempMapper.sumAndAvgOfMotorTemp(date);
    }
}
