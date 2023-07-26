package com.atguigu.car2023sugar.service;

import com.atguigu.car2023sugar.bean.AS;
import com.atguigu.car2023sugar.mapper.AlertMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AlertServiceImpl implements AlertService{

    @Autowired
    AlertMapper alertMapper;

    @Override
    public List<AS> alertSumByLevel(String date) {
        return alertMapper.alertSumByLevel(date);
    }
}
