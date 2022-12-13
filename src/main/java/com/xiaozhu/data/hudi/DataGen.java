package com.xiaozhu.data.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DataGen {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")");


        tableEnv.executeSql("");
    }
}
