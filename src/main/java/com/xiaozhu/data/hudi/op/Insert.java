package com.xiaozhu.data.hudi.op;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

public class Insert {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String targetTable = "t1";
        String basePath = "file:///tmp/t1";

        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");

        DataStream<RowData> dataStream = env.addSource();
        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age INT")
                .column("ts TIMESTAMP(3)")
                .column("`partition` VARCHAR(20)")
                .pk("uuid")
                .partition("partition")
                .options(options);

        builder.sink(dataStream, false); // The second parameter indicating whether the input data stream is bounded
        env.execute("Api_Sink");

    }
}
