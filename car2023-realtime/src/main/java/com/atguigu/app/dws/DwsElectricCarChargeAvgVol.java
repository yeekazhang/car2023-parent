package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.CarChargeAndDisChargeAvgBean;
import com.atguigu.common.Constant;
import com.atguigu.function.DorisMapFunction;
import com.atguigu.function.MapDimFunction;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
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

/**
 * 车辆充电平均电压电流电阻提统计
 */
public class DwsElectricCarChargeAvgVol extends BaseApp {
    public static void main(String[] args) {
        new DwsElectricCarChargeAvgVol().start(
            40011,
            2,
            "DwsElectricCarChargeAvgVol",
            Constant.TOPIC_DWD_ELECTRIC_CHARGING
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析数据, 封装 pojo 中
        SingleOutputStreamOperator<CarChargeAndDisChargeAvgBean> beanStream = parseToPojo(stream);

        // 2. 开窗聚合
        SingleOutputStreamOperator<CarChargeAndDisChargeAvgBean> result = windowAndAgg(beanStream);






        // 2.5 补充维度
        result.map(new MapDimFunction<CarChargeAndDisChargeAvgBean>() {
                @Override
                public String getRowKey(CarChargeAndDisChargeAvgBean bean) {
                    return bean.getVin();
                }

                @Override
                public String getTable() {
                    return "dim_car_info";
                }

                @Override
                public void addDims(CarChargeAndDisChargeAvgBean bean, JSONObject dim) {
                    bean.setCategory(dim.getString("category"));
                    bean.setTrademark(dim.getString("trademark"));
                    bean.setType(dim.getString("type"));
                }
            })
            .print();








        // 3. 写出到 doris 中
       // writeToDoris(result);
    }

    private void writeToDoris(SingleOutputStreamOperator<CarChargeAndDisChargeAvgBean> result) {
        result
            .map(new DorisMapFunction<>())
            .sinkTo(FlinkSinkUtil.getDorisSink("car_db.dws_car_charge_avg"));
    }

    private SingleOutputStreamOperator<CarChargeAndDisChargeAvgBean> windowAndAgg(SingleOutputStreamOperator<CarChargeAndDisChargeAvgBean> beanStream) {
       return beanStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<CarChargeAndDisChargeAvgBean>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
                    .withIdleness(Duration.ofSeconds(120))
            )
            .keyBy(bean -> bean.getVin())
            .window(TumblingEventTimeWindows.of(Time.minutes(3)))
            .reduce(
                new ReduceFunction<CarChargeAndDisChargeAvgBean>() {
                    @Override
                    public CarChargeAndDisChargeAvgBean reduce(CarChargeAndDisChargeAvgBean value1, CarChargeAndDisChargeAvgBean value2) throws Exception {
                        value1.setTotalVol(value1.getTotalVol() + value2.getTotalVol());
                        value1.setTotalElectricCurrent(value1.getTotalElectricCurrent() + value2.getTotalElectricCurrent());
                        value1.setTotalInsulationResistance(value1.getTotalInsulationResistance() + value2.getTotalInsulationResistance());
                        value1.setNum(value1.getNum() + value2.getNum());

                        return value1;
                    }
                },
                new ProcessWindowFunction<CarChargeAndDisChargeAvgBean, CarChargeAndDisChargeAvgBean, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context ctx,
                                        Iterable<CarChargeAndDisChargeAvgBean> elements,
                                        Collector<CarChargeAndDisChargeAvgBean> out) throws Exception {
                        CarChargeAndDisChargeAvgBean bean = elements.iterator().next();

                        bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                        bean.setCur_date(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                        out.collect(bean);
                    }
                }
            );
    }

    private SingleOutputStreamOperator<CarChargeAndDisChargeAvgBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
            .map(JSONObject::parseObject)
            .keyBy(obj -> obj.getString("vin"))
            .process(new KeyedProcessFunction<String, JSONObject, CarChargeAndDisChargeAvgBean>() {
                @Override
                public void processElement(JSONObject value,
                                           Context ctx,
                                           Collector<CarChargeAndDisChargeAvgBean> out) throws Exception {
                    String vin = value.getString("vin");
                    Long ts = value.getLong("timestamp");
                    Long voltage = value.getLong("voltage");
                    Long electricCurrent = value.getLong("electric_current");
                    Long insulationResistance = value.getLong("insulation_resistance");

                    out.collect(CarChargeAndDisChargeAvgBean.builder()
                            .vin(vin)
                            .ts(ts)
                            .totalVol(voltage)
                            .totalElectricCurrent(electricCurrent)
                            .totalInsulationResistance(insulationResistance)
                            .num(1L)
                            .build());
                }
            });
    }
}
