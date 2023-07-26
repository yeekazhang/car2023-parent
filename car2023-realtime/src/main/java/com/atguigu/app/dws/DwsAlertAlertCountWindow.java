package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.AlertCountBean;
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

public class DwsAlertAlertCountWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsAlertAlertCountWindow().start(
                40001,
                2,
                "DwsAlertAlertCountWindow",
                Constant.TOPIC_DWD_ALERT_WARN
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1 转化为pojo
        SingleOutputStreamOperator<AlertCountBean> beanStream = parseToPojo(stream);

        // 2 开窗聚合
        SingleOutputStreamOperator<AlertCountBean> resultStream = windowAndAgg(beanStream);

        // 3 写出到doris
        writeToDoris(resultStream);

    }

    private void writeToDoris(SingleOutputStreamOperator<AlertCountBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("car.dws_alert_alert_count"));

    }

    private SingleOutputStreamOperator<AlertCountBean> windowAndAgg(SingleOutputStreamOperator<AlertCountBean> stream) {
        return stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AlertCountBean>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofHours(2))
                )
                .keyBy(bean -> bean.getVin() + "_" + bean.getAlertLevel())
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .reduce(
                        new ReduceFunction<AlertCountBean>() {
                            @Override
                            public AlertCountBean reduce(AlertCountBean value1, AlertCountBean value2) throws Exception {
                                value1.setAlertCt(value1.getAlertCt() + value2.getAlertCt());
                                return value1;
                            }
                        }, new ProcessWindowFunction<AlertCountBean, AlertCountBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx, Iterable<AlertCountBean> elements, Collector<AlertCountBean> out) throws Exception {
                                AlertCountBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                );

    }

    private SingleOutputStreamOperator<AlertCountBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("vin"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, AlertCountBean>() {

                            private ValueState<Integer> firstAlertState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                firstAlertState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("firstAlert", Integer.class));
                            }

                            @Override
                            public void processElement(JSONObject value, Context ctx, Collector<AlertCountBean> out) throws Exception {
                                Integer firstAlert = firstAlertState.value();
                                Integer alarmSign = value.getInteger("alarm_sign");
                                String vin = value.getString("vin");
                                String alarmLevel = value.getInteger("alarm_level").toString();
                                Long ts = value.getLong("timestamp");
                                Long alertCt = 0L;

                                if(!alarmSign.equals(firstAlert)){
                                    alertCt = 1L;
                                    firstAlertState.update(alarmSign);
                                }

                                out.collect(new AlertCountBean(
                                        "", "",
                                        "",
                                        vin,
                                        alertCt, alarmLevel,
                                        ts
                                ));

                            }
                        }
                );
    }
}


/**
 *
 * 汽车告警次数
 *  怎么判断汽车的告警是这一次还是上一次的？
 */