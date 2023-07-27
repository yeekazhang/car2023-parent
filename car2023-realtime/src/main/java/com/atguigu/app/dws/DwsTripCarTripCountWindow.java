package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.CarTripCountBean;
import com.atguigu.common.Constant;
import com.atguigu.function.DorisMapFunction;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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

public class DwsTripCarTripCountWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTripCarTripCountWindow().start(40009,
            2,
            "DwsTripCarTripCount",
            Constant.TOPIC_DWD_TRIP_JOURNEY);
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析数据, 封装 pojo 中
        SingleOutputStreamOperator<CarTripCountBean> beanStream = parseToPojo(stream);
        // 2. 开窗聚合
//        beanStream.print();
        SingleOutputStreamOperator<CarTripCountBean> result = windowAndAgg(beanStream);
        // 3. 写出到 doris 中
        writeToDoris(result);


    }

    private void writeToDoris(SingleOutputStreamOperator<CarTripCountBean> result) {
        result
            .map(new DorisMapFunction<>())

            .sinkTo(FlinkSinkUtil.getDorisSink("car.dws_car_trip_count"));
    }

    private SingleOutputStreamOperator<CarTripCountBean> windowAndAgg(SingleOutputStreamOperator<CarTripCountBean> beanStream) {
        return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<CarTripCountBean>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withTimestampAssigner((bean,ts) -> bean.getTs())
                    .withIdleness(Duration.ofSeconds(120))
            )
            .keyBy(value -> value.getVin())
            .window(TumblingEventTimeWindows.of(Time.minutes(3)))
            .reduce(
                new ReduceFunction<CarTripCountBean>() {
                    @Override
                    public CarTripCountBean reduce(CarTripCountBean value1,
                                                   CarTripCountBean value2) throws Exception {
                        value1.setOneCount(value1.getOneCount() + value2.getOneCount());
                        value1.setMileage(Math.max(value1.getMileage(), value2.getMileage()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<CarTripCountBean, CarTripCountBean, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<CarTripCountBean> elements, // 有且仅有一个值: 前面聚合的最终结果
                                        Collector<CarTripCountBean> out) throws Exception {
                        CarTripCountBean bean = elements.iterator().next();

                        bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                        out.collect(bean);
                    }
                }

            );
    }

    private SingleOutputStreamOperator<CarTripCountBean> parseToPojo(DataStreamSource<String> stream) {

        return stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("vin"))
            .process(new KeyedProcessFunction<String, JSONObject, CarTripCountBean>() {

                private ValueState<Long> lastMilState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastMilState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("lastMil", Long.class));
                }

                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<CarTripCountBean> out) throws Exception {
                    Long mileage = obj.getLong("mileage");
                    Long ts = obj.getLong("timestamp");

                    String vin = obj.getString("vin");

                    Long lastMil = lastMilState.value();
                    Long oneCount = 0L;
                    if (lastMil == null) {
                        lastMilState.update(mileage);
                    }else {
                        if (lastMil < mileage) {
                            oneCount = mileage - lastMil;
                            lastMilState.update(mileage);
                        }else {
                            mileage = lastMil;
                        }
                    }


                    CarTripCountBean bean = new CarTripCountBean();
                    bean.setTs(ts);
                    bean.setVin(vin);
                    bean.setMileage(mileage);
                    bean.setOneCount(oneCount);


                    out.collect(bean);
                }
            });




    }
}
