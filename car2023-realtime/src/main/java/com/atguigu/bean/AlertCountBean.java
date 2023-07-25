package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlertCountBean {

    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 当前时间
    String curDate;

    // 车辆id
    String vin;

    // 告警次数
    Long AlertCt;

    // 告警等级
    String alertLevel;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;

}
