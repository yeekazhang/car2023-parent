package com.atguigu.bean;


import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnergyChargeBean {

    String vin  ;

    @JSONField(serialize = false)
    Long timestamp  ;

    Integer  chargeCycles;

    String  stt ;

    String  edt;

    String  curDate;


}
