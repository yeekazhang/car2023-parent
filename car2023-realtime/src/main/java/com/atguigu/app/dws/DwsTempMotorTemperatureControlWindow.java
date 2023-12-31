package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.MotorTemperatureControlBean;
import com.atguigu.common.Constant;
import com.atguigu.function.AsyncDimFunction;
import com.atguigu.function.DorisMapFunction;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTempMotorTemperatureControlWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTempMotorTemperatureControlWindow().start(
                40004,
                2,
                "DwsTempMotorTemperatureControlWindow",
                Constant.TOPIC_DWD_TEMP_MOTOR
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 1 转化为 Pojo
        SingleOutputStreamOperator<MotorTemperatureControlBean> beanStream = parseToPojo(stream);


        // 2 开窗和聚合
        SingleOutputStreamOperator<MotorTemperatureControlBean> resultStream = windowAndAgg(beanStream);

        // 3 join 维度
        SingleOutputStreamOperator<MotorTemperatureControlBean> result = joinDim(resultStream);

        // 4 写出到 doris
        writeToDoris(result);

    }

    private SingleOutputStreamOperator<MotorTemperatureControlBean> joinDim(SingleOutputStreamOperator<MotorTemperatureControlBean> resultStream) {
        return AsyncDataStream.unorderedWait(
                resultStream,
                new AsyncDimFunction<MotorTemperatureControlBean>() {
                    @Override
                    public String getRowKey(MotorTemperatureControlBean bean) {
                        return bean.getVin();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_car_info";
                    }

                    @Override
                    public void addDims(MotorTemperatureControlBean bean, JSONObject dim) {
                        bean.setTrademark(dim.getString("trademark"));
                        bean.setCompany(dim.getString("company"));
                        bean.setPowerType(dim.getString("power_type"));
                        bean.setChargeType(dim.getString("charge_type"));
                        bean.setCategory(dim.getString("category"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

    }

    private SingleOutputStreamOperator<MotorTemperatureControlBean> windowAndAgg(SingleOutputStreamOperator<MotorTemperatureControlBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<MotorTemperatureControlBean>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofMinutes(300))
                )
                .keyBy(MotorTemperatureControlBean::getVin)
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .reduce(
                        new ReduceFunction<MotorTemperatureControlBean>() {
                            @Override
                            public MotorTemperatureControlBean reduce(MotorTemperatureControlBean value1, MotorTemperatureControlBean value2) throws Exception {
                                value1.setMotorMaxTemperature(value1.getMotorMaxTemperature() > value2.getMotorMaxTemperature() ? value1.getMotorMaxTemperature() : value2.getMotorMaxTemperature());
                                value1.setControlMaxTemperature(value1.getControlMaxTemperature() > value2.getControlMaxTemperature() ? value1.getControlMaxTemperature() : value2.getControlMaxTemperature());
                                value1.setMotorAccTemperature(value1.getMotorAccTemperature() + value2.getMotorAccTemperature());
                                value1.setControlAccTemperature(value1.getControlAccTemperature() + value2.getControlAccTemperature());
                                value1.setMotorAccCt(value1.getMotorAccCt() + value2.getMotorAccCt());
                                return value1;
                            }
                        }, new ProcessWindowFunction<MotorTemperatureControlBean, MotorTemperatureControlBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx, Iterable<MotorTemperatureControlBean> elements, Collector<MotorTemperatureControlBean> out) throws Exception {
                                MotorTemperatureControlBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                );
    }

    private void writeToDoris(SingleOutputStreamOperator<MotorTemperatureControlBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("car.dws_temp_motor_temperature_control"));
    }


    private SingleOutputStreamOperator<MotorTemperatureControlBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(new MapFunction<String, MotorTemperatureControlBean>() {
                    @Override
                    public MotorTemperatureControlBean map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String vin = obj.getString("vin");
                        Long carNum = 1L;
                        JSONArray motorList = obj.getJSONArray("motor_list");
                        Integer motorMaxTemperature = 0;
                        Integer controlMaxTemperature = 0;
                        Long motorAccTemperature = 0L;
                        Long controlAccTemperature = 0L;
                        for (Object o : motorList) {
                            JSONObject motor = (JSONObject) o;
                            Integer motorTemperature = motor.getInteger("temperature");
                            Integer controlTemperature = motor.getInteger("controller_temperature");
                            // 取机器里最大的温度
                            motorMaxTemperature = motorTemperature > motorMaxTemperature ? motorTemperature : motorMaxTemperature;
                            controlMaxTemperature = controlTemperature > controlMaxTemperature ? controlTemperature : controlMaxTemperature;
                            motorAccTemperature += motorTemperature;
                            controlAccTemperature += controlTemperature;
                        }
                        Long ts = obj.getLong("timestamp");

                        return new MotorTemperatureControlBean(
                                "", "", "",
                                vin, "", "", "", "", "",
                                motorMaxTemperature, controlMaxTemperature,
                                motorAccTemperature, controlAccTemperature,
                                2L,
                                ts
                        );


                    }
                });


    }
}












