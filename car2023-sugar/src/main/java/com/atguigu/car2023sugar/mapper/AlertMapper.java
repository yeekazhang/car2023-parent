package com.atguigu.car2023sugar.mapper;

import com.atguigu.car2023sugar.bean.AS;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface AlertMapper {

    @Select("select alert_level, sum(alert_ct) alert_ct  from dws_alert_alert_count partition(par${date}) group by alert_level order by alert_level")
    List<AS> alertSumByLevel(String date);

}
