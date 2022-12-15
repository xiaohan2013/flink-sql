package com.xiaozhu.data.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> txt = env.readTextFile("");
        DataStream<Tuple2<String, Integer>> counts = txt.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s1 : s.split(",")) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        }).keyBy(s -> s.f0).sum(1);

        counts.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                new TextOutputFormat(new Path("")).writeRecord(value);
            }
        });

        env.execute("WordCount");
    }
}
