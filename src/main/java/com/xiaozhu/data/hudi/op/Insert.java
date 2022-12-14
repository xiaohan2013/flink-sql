package com.xiaozhu.data.hudi.op;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class Insert {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 将其写到Kafka
        tableEnv.executeSql("-- 创建kafka动态source\n" +
                "CREATE TABLE wzp.kafka_monitor (\n" +
                "  `partition` INT METADATA VIRTUAL,\n" +
                "  `offset` BIGINT METADATA VIRTUAL,\n" +
                "  `timestamp` TIMESTAMP(3) METADATA VIRTUAL,\n" +
                "  `svc_id` STRING,\n" +
                "  `type` STRING,\n" +
                "  `app` STRING,\n" +
                "  `url` STRING,\n" +
                "  `span` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'monitor-log',\n" +
                "  'properties.bootstrap.servers' = '10.0.0.1:9092,10.0.0.2:9092',\n" +
                "  'properties.group.id' = 'wzp_test_flink',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ");\n" +
                " \n" +
                "-- 创建hudi sink\n" +
                "USE hudi_flink;\n" +
                "CREATE TABLE rest_response_mor(\n" +
                "  `svc_id` STRING PRIMARY KEY NOT ENFORCED,\n" +
                "  `url` STRING,\n" +
                "  `span` INT,\n" +
                "  `timestamp` TIMESTAMP(3),\n" +
                "  `app` STRING\n" +
                ")\n" +
                "PARTITIONED BY (`app`)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'hdfs://flink/huditables/rest_response_mor',\n" +
                "  'table.type' = 'MERGE_ON_READ', -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE\n" +
                "  'hoodie.datasource.write.recordkey.field' = 'svc_id',\n" +
                "  'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
                "  'hoodie.datasource.write.partitionpath.urlencode' = 'true',\n" +
                "  'hoodie.parquet.compression.codec'= 'snappy',\n" +
                "  'write.operation' = 'upsert',\n" +
                "  'write.precombine' = 'true',\n" +
                "  'write.precombine.field' = 'timestamp',\n" +
                "  'compaction.async.enabled' = 'true',\n" +
                "  'compaction.trigger.strategy' = 'num_and_time',\n" +
                "  'compaction.delta_commits' = '3',\n" +
                "  'compaction.delta_seconds' = '30',\n" +
                "  'hive_sync.enable' = 'true',\n" +
                "  'hive_sync.use_jdbc' = 'false',\n" +
                "  'hive_sync.mode' = 'hms',\n" +
                "  'hive_sync.metastore.uris' = 'thrift://machine1:9083',\n" +
                "  'hive_sync.db' = 'hudi_external',\n" +
                "  'hive_sync.table' = 'rest_response_mor',\n" +
                "  'hive_sync.assume_date_partitioning' = 'false',\n" +
                "  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.HiveStylePartitionValueExtractor', -- 默认按日期分区\n" +
                "  'hive_sync.partition_fields' = 'app',\n" +
                "  'hive_sync.support_timestamp'= 'true'\n" +
                ");\n" +
                "\n" +
                "-- 开启checkpoint才会写入数据到hudi sink\n" +
                "set execution.checkpointing.interval=10sec;\n" +
                "insert into hudi_flink.rest_response_mor(`svc_id`,`url`,`span`,`timestamp`,`app`) select `svc_id`,`url`,`span`,`timestamp`,`app` from wzp.kafka_monitor;\n");
    }
}
