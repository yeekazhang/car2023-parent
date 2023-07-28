package com.atguigu.car2023sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.car2023sugar.bean.AS;
import com.atguigu.car2023sugar.bean.MotorTemp;
import com.atguigu.car2023sugar.service.AlertService;
import com.atguigu.car2023sugar.service.TempService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class SugarController {

    @Autowired
    AlertService alertService;


    @Autowired
    TempService tempService;


    // 不用能源类型当天的电机
    @RequestMapping("/sugar/temp/motorTemp")
    public String sumAndAvgOfMotorTemp(String date){
        if(date == null){
            date = new SimpleDateFormat("yyyyMMdd").format(new Date());
        }

        List<MotorTemp> list = tempService.sumAndAvgOfMotorTemp(date);

        List<String> colors = Arrays.asList("#f05050", "#3F63B9", "#7BE5C9");

        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", 0);
        JSONArray data = new JSONArray();

        for (int i = 0; i < list.size(); i++) {
            JSONObject innerData = new JSONObject();
            innerData.put("dimValue", date);
            JSONArray indicators = new JSONArray();

            // 第一个数据
            JSONObject ind1 = new JSONObject();
            JSONObject ind2 = new JSONObject();
            JSONObject ind3 = new JSONObject();
            JSONObject ind4 = new JSONObject();
            JSONObject ind5 = new JSONObject();

            ind1.put("name", "能源类型");
            ind1.put("value", list.get(i).getPower_type());
            ind1.put("rate_level", "custom");
            ind1.put("color", colors.get(i));

            ind2.put("name", "电机最大温度");
            ind2.put("value", list.get(i).getMotor_max_temperature().toString());

            ind3.put("name", "电机平均温度");
            ind3.put("value", list.get(i).getMotor_avg_temperature().toString());

            ind4.put("name", "控制器最大温度");
            ind4.put("value", list.get(i).getControl_max_temperature().toString());

            ind5.put("name", "控制器平均温度");
            ind5.put("value", list.get(i).getControl_avg_temperature().toString());

            indicators.add(ind1);
            indicators.add(ind2);
            indicators.add(ind3);
            indicators.add(ind4);
            indicators.add(ind5);

            innerData.put("indicators", indicators);
            data.add(innerData);
        }

        result.put("data", data);
        return result.toJSONString();


    }


    // 不同等级的一天告警次数
    @RequestMapping("/sugar/alert/alertSumByLevel")
    public String alertSumByLevel(String date){
        if(date == null){
            date = new SimpleDateFormat("yyyyMMdd").format(new Date());
        }

        List<AS> list = alertService.alertSumByLevel(date);

        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", 0);

        JSONObject data = new JSONObject();

        JSONArray categories = new JSONArray();
        for (AS as : list) {
            categories.add(as.getAlert_level());
        }
        data.put("categories", categories);

        JSONArray series = new JSONArray();

        JSONObject obj = new JSONObject();
        obj.put("name", "告警等级");
        JSONArray innerData = new JSONArray();
        for (AS as : list) {
            innerData.add(as.getAlert_ct());
        }
        obj.put("data", innerData);
        series.add(obj);

        data.put("series", series);

        result.put("data", data);

        return result.toJSONString();

    }

}
