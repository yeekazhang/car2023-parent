package com.atguigu.car2023sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.car2023sugar.bean.AS;
import com.atguigu.car2023sugar.service.AlertService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class SugarController {

    @Autowired
    AlertService alertService;


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
