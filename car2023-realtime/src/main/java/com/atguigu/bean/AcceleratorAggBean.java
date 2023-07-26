package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AcceleratorAggBean {

    String  stt;

    String  edt;

    String  curDate;

    String  vin;
    @Builder.Default
    Double sumElectric = 0.0;
    @Builder.Default
    Integer electricCount =0;

    @JSONField(serialize = false)
    Long  timestamp;

    @JSONField(serialize = false)
    Double  electricCurrent;





}
