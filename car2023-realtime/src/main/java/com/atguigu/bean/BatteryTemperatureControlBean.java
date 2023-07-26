package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class BatteryTemperatureControlBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 当前时间
    String curDate;

    // 汽车id
    String vin;

    // 电池最高温度
    Integer maxTemperature;

    // 电池温度异常值次数
    Long batteryAbnormalCt;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
