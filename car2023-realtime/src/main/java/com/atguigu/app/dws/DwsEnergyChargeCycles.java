package com.atguigu.app.dws;

import com.atguigu.app.BaseApp;
import com.atguigu.bean.EnergyChargeBean;
import com.atguigu.common.Constant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsEnergyChargeCycles extends BaseApp {

    public static void main(String[] args) {
        new DwsEnergyChargeCycles().start(
                40020,
                "DwsEnergyChargeCycles",
                Constant.TOPIC_DWD_ENERGY_CHARGE

        );
    }


    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {

        /*
        * 能耗域各汽车充电次数 */

        //1.解析成pojo
        parseToPoJo(stream);

        //2.开窗

        //3.聚合


    }

    private void parseToPoJo(DataStreamSource<String> stream) {

        stream
                .map(new MapFunction<String, EnergyChargeBean>() {
                    @Override
                    public EnergyChargeBean map(String value) throws Exception {
                        return  JSON;
                    }
                })


    }
}
