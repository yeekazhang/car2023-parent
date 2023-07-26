package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.AcceleratorAggBean;
import com.atguigu.common.Constant;
import com.atguigu.function.DorisMapFunction;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import javafx.scene.input.DataFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.doris.shaded.org.apache.arrow.flatbuf.Int;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsAcceleratorAggElectricWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsAcceleratorAggElectricWindow().start(
                40022,
                2,
                "DwsAcceleratorAggElectricWindow",
                Constant.TOPIC_DWD_TEMP_MOTOR
        );

    }

    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {

        stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("vin"))
                .process(new KeyedProcessFunction<String, JSONObject, AcceleratorAggBean>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<AcceleratorAggBean> out) throws Exception {

                        String vin = obj.getString("vin");
                        Long timestamp = obj.getLong("timestamp");
                        JSONArray motor_list = obj.getJSONArray("motor_list");

                        JSONObject jsonObject0 = motor_list.getJSONObject(0);
                        JSONObject jsonObject1 = motor_list.getJSONObject(1);
                        Double electricCurrent = (jsonObject0.getInteger("electric_current")+jsonObject1.getInteger("electric_current")+0.0)/2;


                        out.collect(AcceleratorAggBean.builder()
                                        .vin(vin)
                                        .timestamp(timestamp)
                                        .electricCurrent(electricCurrent)
                                        .electricCount(1)
                                        .sumElectric(electricCurrent)
                                .build());


                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AcceleratorAggBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((bean,ts) -> bean.getTimestamp())
                        .withIdleness(Duration.ofSeconds(120)))
                .keyBy(AcceleratorAggBean::getVin)
                .window(TumblingEventTimeWindows.of(Time.seconds(10*60)))
                .reduce(new ReduceFunction<AcceleratorAggBean>() {
                    @Override
                    public AcceleratorAggBean reduce(AcceleratorAggBean value1, AcceleratorAggBean value2) throws Exception {

                        value1.setElectricCount(value1.getElectricCount() + value2.getElectricCount());
                        value1.setSumElectric(value1.getSumElectric() + value2.getSumElectric());

                        return value1;
                    }
                }, new ProcessWindowFunction<AcceleratorAggBean, AcceleratorAggBean, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context ctx,
                                        Iterable<AcceleratorAggBean> elements,
                                        Collector<AcceleratorAggBean> out) throws Exception {

                        AcceleratorAggBean bean = elements.iterator().next();

                        bean.setStt(DateFormatUtil.tsToDate(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDate(ctx.window().getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                        out.collect(bean);

                    }
                })
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("car.dws_accelerator_agg_electric_window"));








    }




}






































