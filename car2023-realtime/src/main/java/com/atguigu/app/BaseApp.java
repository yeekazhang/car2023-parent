package com.atguigu.app;


import com.atguigu.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    protected abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void start(int port, int p, String ckAndGroupId, String topic) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        Configuration conf = new Configuration();
        // rest.port flink会在制定的端口启动一个http服务器，是的用户可以使用rest app与flink进行交互，执行任务和作业
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        // 1. 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 2. 开启 checkpoint
        env.enableCheckpointing(3000);
        // 3. 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 4. checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/car2023/" + ckAndGroupId);
        // 5. checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 6. checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 7. checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 8. job 取消的时候的, checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 获取流之后, 具体的处理和业务逻辑相关
        handle(env, stream);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();

        }

    }

}
