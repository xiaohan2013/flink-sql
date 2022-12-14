package com.xiaozhu.data.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DataGen {
    public static void DataSource(TableEnvironment env){
        env.executeSql("CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                " 'rows-per-second' = '2'\n" +
                ")");
    }
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    proctime AS PROCTIME(),   -- generates processing-time attribute using computed column\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',  -- using kafka connector\n" +
                "    'topic' = 'user_behavior',  -- kafka topic\n" +
                "    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning\n" +
                "    'properties.bootstrap.servers' = 'kafka:9094',  -- kafka broker address\n" +
                "    'format' = 'json'  -- the data format is json\n" +
                ")\n");


    }
}
