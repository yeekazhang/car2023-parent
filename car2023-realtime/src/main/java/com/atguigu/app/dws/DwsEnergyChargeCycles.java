package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.EnergyChargeBean;
import com.atguigu.common.Constant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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

    private SingleOutputStreamOperator<EnergyChargeBean> parseToPoJo(DataStreamSource<String> stream) {

       return stream
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSONObject.parseObject(value);



                    }
                })
                .keyBy(bean -> bean.getString("vin"))
                .process(new KeyedProcessFunction<String, JSONObject, EnergyChargeBean>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx, 
                                               Collector<EnergyChargeBean> out) throws Exception {
                        String vin = obj.getString("vin");
                        Long timestamp = obj.getLong("timestamp");

                        out.collect(new EnergyChargeBean(vin,timestamp,0,"","",""));

                        
                    }
                });


    }
}
