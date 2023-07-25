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

    Integer  chargeCycles;

    Integer  chargeSlowCycles;

    Integer  chargeFastCycles;


    String  stt ;

    String  edt;

    String  curDate;

    @JSONField(serialize = false)
    Long timestamp  ;

    @JSONField(serialize = false)
    Integer charge_status  ;

    @JSONField(serialize = false)
    Integer  electric_current;


}
