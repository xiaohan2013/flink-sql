package com.xiaozhu.data.hudi;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

public class FlinkHudiMain {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment streamTableEnv = TableEnvironmentImpl.create(settings);
        streamTableEnv.getConfig().getConfiguration()
                .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        Configuration execConf = streamTableEnv.getConfig().getConfiguration();
        execConf.setString("execution.checkpointing.interval", "2s");
        // configure not to retry after failure
        execConf.setString("restart-strategy", "fixed-delay");
        execConf.setString("restart-strategy.fixed-delay.attempts", "0");

        // 创建数据源表
        DataGen.DataSource(streamTableEnv);

        // 创建Hudi表
        streamTableEnv.executeSql("create table dwd_orders_cow (\n" +
                "    order_number BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    first_name   STRING,\n" +
                "    last_name STRING,\n" +
                "    order_time   TIMESTAMP(3),\n" +
                "    proctime AS PROCTIME(), \n" +
                "WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND \n" +
                        ")with(" +
                        "'connector' = 'hudi',\n" +
                        "'path'='hdfs://localhost:9000/hudi/dwd_orders_cow',\n" +
                        "'table.type'='COPY_ON_WRITE', \n" +
                        "'hoodie.datasource.write.recordkey.field' = 'order_number',\n" +
                        "'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator',\n" +
                        "'preCombineField'='order_time',\n" +
                        "'hoodie.datasource.write.hive_style_partitioning'='true'\n" +
                        ")\n"
        );

        // 写入数据
        streamTableEnv.executeSql("insert into dwd_orders_cow select  order_number, price, buyer.first_name as first_name, buyer.last_name as last_name, order_time from Orders");

        // 查询数据

    }
}
