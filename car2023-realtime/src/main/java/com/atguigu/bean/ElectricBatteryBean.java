package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ElectricBatteryBean {

    String stt;

    String edt;

    String curDate;


    String vin;


    @Builder.Default
    Integer batteryMaxDiffVoltage =0;
    @Builder.Default
    Integer batteryVoltageWarnTimes =0;


    @JSONField(serialize = false)
    Long timestamp ;

    @JSONField(serialize = false)
    Integer maxVoltage;
    @Builder.Default
    @JSONField(serialize = false)
    Integer minVoltage = 0;

    public static void main(String[] args) {

        ElectricBatteryBean electricBatteryBean = new ElectricBatteryBean();
        System.out.println(electricBatteryBean);

        String n = null ;
       if( n.equals("cc") ){

       }
        System.out.println(n);

    }



}
