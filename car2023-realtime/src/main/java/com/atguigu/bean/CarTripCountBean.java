package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class CarTripCountBean {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String cur_date;
    // 汽车id
    private String vin ;
    //总里程
    private Long mileage ;


    //当次行驶累计里程
    private Long oneCount;



    // 时间戳
    @JSONField(serialize = false)  // 要不要序列化这个字段
    private Long ts;

}
