package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MotorTemperatureControlBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 当前时间
    String curDate;

    // 汽车id
    String vin;

    // 电机最高温度
    Integer motorMaxTemperature;

    // 控制器最高温度
    Integer controlMaxTemperature;

    // 电机平均温度
    Long motorAvgTemperature;

    // 控制器平均温度
    Long controlAvgTemperature;

    // 电机温度累计值
    @JSONField(serialize = false)
    Long motorAccTemperature;

    // 控制器温度累计值
    @JSONField(serialize = false)
    Long controlAccTemperature;

    // 电机数量累计值
    @JSONField(serialize = false)
    Long motorAccCt;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;

}
