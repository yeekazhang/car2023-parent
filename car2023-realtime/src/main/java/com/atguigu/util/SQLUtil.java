package com.atguigu.util;

import com.atguigu.common.Constant;

public class SQLUtil {
    public static String getKafkaSource(String topic, String groupId) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," +
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic) {
        return "with(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.json.ignore-parse-errors' = 'true'," +
                "  'value.json.ignore-parse-errors' = 'true'," +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}
