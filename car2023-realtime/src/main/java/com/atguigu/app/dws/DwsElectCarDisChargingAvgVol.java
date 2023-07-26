package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.CarDisChargingAvgVolBean;
import com.atguigu.common.Constant;
import com.atguigu.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsElectCarDisChargingAvgVol extends BaseApp {
    public static void main(String[] args) {
        new DwsElectCarDisChargingAvgVol().start(
            40010,
            2,
            "CarChargingAverageVoltage",
            Constant.TOPIC_DWD_ELECTRIC_DISCHARGING
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析数据, 封装 pojo 中
        SingleOutputStreamOperator<CarDisChargingAvgVolBean> beanStream = parseToPojo(stream);

        // 2. 开窗聚合
        windowAndAgg(beanStream);
        // 3. 写出到 doris 中
    }

    private void windowAndAgg(SingleOutputStreamOperator<CarDisChargingAvgVolBean> beanStream) {
        beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<CarDisChargingAvgVolBean>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
                    .withIdleness(Duration.ofSeconds(120))
            )
            .keyBy(bean -> bean.getVin())
            .window(TumblingEventTimeWindows.of(Time.minutes(3)))
            .reduce(
                new ReduceFunction<CarDisChargingAvgVolBean>() {
                    @Override
                    public CarDisChargingAvgVolBean reduce(CarDisChargingAvgVolBean value1, CarDisChargingAvgVolBean value2) throws Exception {
                        value1.setTotalVol(value1.getTotalVol() + value2.getTotalVol());
                        value1.setNum(value1.getNum() + value2.getNum());

                        return value1;
                    }
                },
                new ProcessWindowFunction<CarDisChargingAvgVolBean, CarDisChargingAvgVolBean, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context ctx,
                                        Iterable<CarDisChargingAvgVolBean> elements,
                                        Collector<CarDisChargingAvgVolBean> out) throws Exception {
                        CarDisChargingAvgVolBean bean = elements.iterator().next();

                        bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                        bean.setCur_date(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                        out.collect(bean);
                    }
                }
            ).print();
    }

    private SingleOutputStreamOperator<CarDisChargingAvgVolBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
            .map(JSONObject::parseObject)
            .keyBy(obj -> obj.getString("vin"))
            .process(new KeyedProcessFunction<String, JSONObject, CarDisChargingAvgVolBean>() {
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<CarDisChargingAvgVolBean> out) throws Exception {
                    String vin = value.getString("vin");
                    Long ts = value.getLong("timestamp");
                    Long voltage = value.getLong("voltage");

                    out.collect(new CarDisChargingAvgVolBean("","","",vin,voltage,1L,ts));
                }
            });
    }
}
