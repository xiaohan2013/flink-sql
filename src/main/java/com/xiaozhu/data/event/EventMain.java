package com.xiaozhu.data.event;

import lombok.val;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class EventMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setParallelism(1);

        DataStream dataStream = env.addSource(new TextSource("D:\\java-demo\\flink-sql\\data\\clicks.txt"));

        DataStream<Tuple2<String, Long>> dataStream1 = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String o, Collector collector) throws Exception {
                String[] splits = o.split(",");
                if ("pv".equals(splits[3])) {
                    Tuple2 res = new Tuple2<>(splits[0] + "-" + splits[1], Long.parseLong(splits[4]));
                    collector.collect(res);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> s, long l) {
                        return s.f1;
                    }
                }));


        // key ??????
        dataStream1.keyBy(s -> s.f0).process(new KeyedProcessFunction<String, Tuple2<String, Long>, Object>() {
            // ????????? key ???????????????????????????
            // ?????????????????????????????????????????? MapState
            private ValueState<UserBehavior> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                // ???????????????????????????
                ValueStateDescriptor stateDescriptor = new ValueStateDescriptor<>("mystate", UserBehavior.class);
                // ?????????????????????????????????????????????????????????????????????
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(60)).build());
                // ?????? Keyed State ????????????
                state = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Long> source,
                                       KeyedProcessFunction<String, Tuple2<String, Long>, Object>.Context context,
                                       Collector<Object> collector) throws Exception {
                UserBehavior ub = state.value();
                //
                if(ub == null) {
                    ub = new UserBehavior(source.f0, source.f1);
                    // ??????state
                    state.update(ub);
                    // ???????????????????????????60 ??????????????????????????????
                    // ????????? 60 ??????????????????????????????????????????
                    // ???????????? 60 ?????????????????? ??? ???????????????????????????????????????
                    context.timerService().registerEventTimeTimer(ub.getTimestamp() + 60000);
                    // ???????????????????????????
                    collector.collect(ub);
                } else {
                    System.out.println("[Duplicate Data] " + source.f0 + " " + source.f1);
                }
            }

            @Override
            public void onTimer(long timestamp,
                                OnTimerContext ctx,
                                Collector<Object> out) throws Exception {
                UserBehavior cur = state.value();
                // ?????????????????????????????????
                if (cur.getTimestamp() + 60000 <= timestamp) {
                    System.out.printf("[Overdue] now: %d obj_time: %d Date: %s%n",
                            timestamp, cur.getTimestamp(), cur.getId());
                    state.clear();
                }
            }

            @Override
            public void close() throws Exception {
            }
        });

        env.execute("flink");
    }
}
