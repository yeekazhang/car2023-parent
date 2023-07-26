package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.ElectricBatteryBean;
import com.atguigu.common.Constant;
import com.atguigu.function.DorisMapFunction;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.doris.shaded.org.apache.arrow.flatbuf.Int;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsElectricBatteryWindow extends BaseApp {
    public static void main(String[] args) {

        new DwsElectricBatteryWindow().start(
                40019,
                2,
                "DwsElectricBatteryWindow",
                Constant.TOPIC_DWD_ALERT_WARN
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        stream
                .map(JSON::parseObject)
                .keyBy( obj -> obj.getString("vin"))
                .process(new KeyedProcessFunction<String, JSONObject, ElectricBatteryBean>() {

                    private ValueState<ElectricBatteryBean> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<ElectricBatteryBean> desc = new ValueStateDescriptor<ElectricBatteryBean>("info",ElectricBatteryBean.class);
                        state = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<ElectricBatteryBean> out) throws Exception {
                        String  vin  = obj.getString("vin");
                        Long timestamp = obj.getLong("timestamp");
                        Integer max_voltage = obj.getInteger("max_voltage");
                        Integer min_voltage = obj.getInteger("min_voltage");

                        Integer  batteryVoltageWarnTimes = 0;
                        Integer batteryMaxDiffVoltage = 0;
                        Integer maxVoltage = 0;



                        ElectricBatteryBean infoBean = state.value();
                        if (infoBean == null) {
                            infoBean = new ElectricBatteryBean();
                        }
                        System.out.println(">>>>>>>>>>>>"+infoBean);

                        if(infoBean.getBatteryMaxVoltage() > max_voltage){
                            infoBean.setBatteryMaxVoltage(max_voltage);
                        }

                        maxVoltage = infoBean.getBatteryMaxVoltage();

                        if (infoBean.getMinVoltage()  > min_voltage) {

                            infoBean.setMinVoltage(min_voltage);
                        }

                        batteryMaxDiffVoltage = infoBean.getBatteryMaxDiffVoltage() - infoBean.getMinVoltage();

                        if(max_voltage > 35 ){
                            batteryVoltageWarnTimes =1 ;
                        }


                        out.collect(ElectricBatteryBean.builder()
                                        .vin(vin)
                                        .maxVoltage(max_voltage)
                                        .minVoltage(min_voltage)
                                        .batteryVoltageWarnTimes(batteryVoltageWarnTimes)
                                        .batteryMaxDiffVoltage(batteryMaxDiffVoltage)
                                        .batteryMaxVoltage(maxVoltage)
                                        .timestamp(timestamp)
                                         .build());
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ElectricBatteryBean>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner( (bean,ts) -> bean.getTimestamp())
                        .withIdleness(Duration.ofSeconds(120)))
                .keyBy(ElectricBatteryBean::getVin)
                .window(TumblingEventTimeWindows.of(Time.seconds(5*60)))
                .reduce(new ReduceFunction<ElectricBatteryBean>() {
                    @Override
                    public ElectricBatteryBean reduce(ElectricBatteryBean value1,
                                                      ElectricBatteryBean value2) throws Exception {
                        if (value1.getBatteryMaxVoltage() > value2.getBatteryMaxVoltage()) {
                            value1.setBatteryMaxVoltage(value2.getBatteryMaxVoltage());
                        }
                        if (value1.getBatteryMaxDiffVoltage() > value2.getBatteryMaxDiffVoltage()) {
                            value1.setBatteryMaxDiffVoltage(value2.getBatteryMaxDiffVoltage());
                        }

                        value1.setBatteryVoltageWarnTimes(value1.getBatteryVoltageWarnTimes() + value2.getBatteryVoltageWarnTimes());


                        return value1;
                    }
                }, new ProcessWindowFunction<ElectricBatteryBean, ElectricBatteryBean, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context ctx,
                                        Iterable<ElectricBatteryBean> elements,
                                        Collector<ElectricBatteryBean> out) throws Exception {
                        ElectricBatteryBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDate(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDate(ctx.window().getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));


                        out.collect(bean);


                    }
                }).print();
               /* .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("car.dws_electric_battery_window"));
*/
  }
}
