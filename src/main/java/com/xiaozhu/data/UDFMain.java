package com.xiaozhu.data;

import com.xiaozhu.data.udf.UserScalarFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class UDFMain {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);
//        tableEnv.getConfig().set("parallelism.default","1");

        tableEnv.createTemporarySystemFunction("user_scalar_func", new UserScalarFunction());

        // 创建数据源表
        tableEnv.executeSql("CREATE TABLE source_table (\n" +
                "    user_id BIGINT NOT NULL COMMENT '用户 id'\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '10'\n" +
                ");");

        // tableEnv.executeSql("SHOW FUNCTIONS;").print();

        // 创建数据汇表:sink
//        tableEnv.executeSql("CREATE TABLE sink_table (\n" +
//                "    result_row_1 ROW<age INT, name STRING, totalBalance DECIMAL(10, 2)>,\n" +
//                "    result_row_2 STRING\n" +
//                ") WITH (\n" +
//                "  'connector' = 'print'\n" +
//                ");\n");
//
//        tableEnv.executeSql("insert into sink_table select user_scalar_func(user_id) as result_row_1, " +
//                "user_scalar_func(user_scalar_func(user_id)) as result_row_2 from source_table;\n").print();

        // Sink Hudi
        tableEnv.executeSql("create table sink_hudi(" +
                "    result_row_1 ROW<age INT, name STRING, totalBalance DECIMAL(10, 2)>,\n" +
                "    result_row_2 STRING\n" +
                ") with (" +
                "'connector' = 'hudi',\n " +
                "'path' = 'hdfs://localhost:9000/hudi/user',\n " +
                "'table.type' = 'MERGE_ON_READ',\n " +
                "'write.tasks'='1',\n" +
                "'comaction.tasks' = '1',\n" +
                "'hoodie.datasource.write.recordkey.field' = 'result_row_1',\n" +
                "'hoodie.parquet.compression.codec'= 'snappy'\n" +
                ");\n");

        tableEnv.executeSql("insert into sink_hudi select user_scalar_func(user_id) as result_row_1, " +
                "user_scalar_func(user_scalar_func(user_id)) as result_row_2 from source_table;\n");
    }
}
