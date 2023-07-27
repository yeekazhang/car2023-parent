package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.BatteryTemperatureControlBean;
import com.atguigu.common.Constant;
import com.atguigu.function.AsyncDimFunction;
import com.atguigu.function.DorisMapFunction;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
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
import java.util.concurrent.TimeUnit;

/**
 * 各汽车电池最高温度和异常值次数
 */
public class DwsTempBatteryTemperatureControlWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTempBatteryTemperatureControlWindow().start(
                40003,
                2,
                "DwsTempBatteryTemperatureControlWindow",
                Constant.TOPIC_DWD_TEMP_BATTERY
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 1 过滤并转化为 Pojo
        SingleOutputStreamOperator<BatteryTemperatureControlBean> beanStream = parseToPojo(stream);

        // 2 开窗并聚合
        SingleOutputStreamOperator<BatteryTemperatureControlBean> resultStream = windowAndAgg(beanStream);

        // 3 以异步操作 join 维度
        SingleOutputStreamOperator<BatteryTemperatureControlBean> result = joinDim(resultStream);

        // 4 写入到 Doris
        writeToDoris(result);

    }

    private SingleOutputStreamOperator<BatteryTemperatureControlBean> joinDim(SingleOutputStreamOperator<BatteryTemperatureControlBean> resultStream) {
        return AsyncDataStream.unorderedWait(
                resultStream,
                new AsyncDimFunction<BatteryTemperatureControlBean>() {
                    @Override
                    public String getRowKey(BatteryTemperatureControlBean bean) {
                        return bean.getVin();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_car_info";
                    }

                    @Override
                    public void addDims(BatteryTemperatureControlBean bean, JSONObject dim) {
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

    private void writeToDoris(SingleOutputStreamOperator<BatteryTemperatureControlBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("car.dws_temp_battery_temperature_control"));
    }

    private SingleOutputStreamOperator<BatteryTemperatureControlBean> windowAndAgg(SingleOutputStreamOperator<BatteryTemperatureControlBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<BatteryTemperatureControlBean>forBoundedOutOfOrderness(Duration.ofMinutes(20))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofMinutes(120))
                )
                .keyBy(BatteryTemperatureControlBean::getVin)
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .reduce(
                        new ReduceFunction<BatteryTemperatureControlBean>() {
                            @Override
                            public BatteryTemperatureControlBean reduce(BatteryTemperatureControlBean value1, BatteryTemperatureControlBean value2) throws Exception {
                                value1.setMaxTemperature(value1.getMaxTemperature() > value2.getMaxTemperature() ? value1.getMaxTemperature() : value2.getMaxTemperature());
                                value1.setBatteryAbnormalCt(value1.getBatteryAbnormalCt() + value2.getBatteryAbnormalCt());
                                return value1;
                            }
                        }, new ProcessWindowFunction<BatteryTemperatureControlBean, BatteryTemperatureControlBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx, Iterable<BatteryTemperatureControlBean> elements, Collector<BatteryTemperatureControlBean> out) throws Exception {
                                BatteryTemperatureControlBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                );


    }

    private SingleOutputStreamOperator<BatteryTemperatureControlBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("vin"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, BatteryTemperatureControlBean>() {

                            private ValueState<Boolean> firstAbnormalState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                firstAbnormalState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("firstAbnormal", Boolean.class));
                            }

                            @Override
                            public void processElement(JSONObject value, Context ctx, Collector<BatteryTemperatureControlBean> out) throws Exception {
                                Boolean isFirstAbnormal = firstAbnormalState.value();
                                Long ts = value.getLong("timestamp");
                                String vin = value.getString("vin");
                                Integer maxTemperature = value.getInteger("max_temperature");
                                Long batteryAbnormalCt = 0L;

                                if (maxTemperature > 350) {
                                    if (isFirstAbnormal == null || !isFirstAbnormal) {
                                        batteryAbnormalCt = 1L;
                                        firstAbnormalState.update(true);
                                    }
                                } else {
                                    firstAbnormalState.update(false);
                                }

                                out.collect(
                                        new BatteryTemperatureControlBean(
                                                "", "",
                                                "",
                                                vin, "", "", "", "", "",
                                                maxTemperature, batteryAbnormalCt,
                                                ts
                                        )
                                );

                            }
                        }
                );


    }
}
