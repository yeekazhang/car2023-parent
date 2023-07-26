package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class CarChargeAndDisChargeAvgBean {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String cur_date;
    // 汽车id
    private String vin ;

    //平均电压分子
    private Long totalVol;

    //平均电流分子
    private Long totalElectricCurrent;

    //平均绝缘电阻分子
    private Long totalInsulationResistance;

    //平均分母
    private Long num;




    // 时间戳
    @JSONField(serialize = false)  // 要不要序列化这个字段
    private Long ts;
}
