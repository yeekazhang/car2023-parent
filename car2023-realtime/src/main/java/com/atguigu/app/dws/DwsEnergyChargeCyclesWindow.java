package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseApp;
import com.atguigu.bean.EnergyChargeBean;
import com.atguigu.common.Constant;
import com.atguigu.function.DorisMapFunction;
import com.atguigu.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsEnergyChargeCyclesWindow  extends BaseApp {

    public static void main(String[] args) {
        new DwsEnergyChargeCyclesWindow().start(
                40020,
                2,
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
        SingleOutputStreamOperator<EnergyChargeBean> beanStream = parseToPoJo(stream);
        //2.开窗聚合
        SingleOutputStreamOperator<EnergyChargeBean> resultStream = windowAndAgg(beanStream);

        //3.写到doris
        writeToDoris(resultStream);


    }

    private void writeToDoris(SingleOutputStreamOperator<EnergyChargeBean> resultStream) {

        resultStream.map(new DorisMapFunction())



    }

    private SingleOutputStreamOperator<EnergyChargeBean> windowAndAgg(SingleOutputStreamOperator<EnergyChargeBean> beanStream) {

      return   beanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EnergyChargeBean>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((bean,ts) -> bean.getTimestamp())
                        .withIdleness(Duration.ofSeconds(120))
                )
                .keyBy(EnergyChargeBean::getVin)
                .window(TumblingEventTimeWindows.of(Time.seconds(10*60)))
                .reduce(new ReduceFunction<EnergyChargeBean>() {
                            @Override
                            public EnergyChargeBean reduce(EnergyChargeBean value1, EnergyChargeBean value2) throws Exception {

                                value1.setChargeCycles(value1.getChargeCycles() + value2.getChargeCycles());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<EnergyChargeBean, EnergyChargeBean, String, TimeWindow>() {
                            @Override
                            public void process(String  vin ,
                                                Context ctx,
                                                Iterable<EnergyChargeBean> elements,
                                                Collector<EnergyChargeBean> out) throws Exception {

                                EnergyChargeBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDate(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDate(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));
                                out.collect(bean);


                            }
                        });
    }


    private SingleOutputStreamOperator<EnergyChargeBean> parseToPoJo(DataStreamSource<String> stream) {

       return stream
                .map(JSONObject::parseObject)

                .process(new ProcessFunction<JSONObject, EnergyChargeBean>() {

                    private ValueState<Boolean> isOnceState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<Boolean>("isOnce",Boolean.class);
                        isOnceState = getRuntimeContext().getState(desc);

                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<EnergyChargeBean> out) throws Exception {
                        String vin = obj.getString("vin");
                        Integer charge_status = obj.getInteger("charge_status");
                        Long timestamp = obj.getLong("timestamp");
                        Integer chargeCycles = 0;
                        Boolean isOnceStateValue  = isOnceState.value();

                       if( isOnceStateValue == null || isOnceStateValue ) {

                           if (charge_status == 1 || charge_status == 2) {
                               chargeCycles = 1;
                               isOnceState.update(false);

                           }
                       }

                       if(charge_status == 3 ||  charge_status == 4){
                           isOnceState.update(true);
                       }


                        out.collect(new EnergyChargeBean(vin,chargeCycles,"","","",timestamp,0));
                    }
                });


    }
}
