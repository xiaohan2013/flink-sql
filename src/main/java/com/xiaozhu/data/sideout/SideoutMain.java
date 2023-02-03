package com.xiaozhu.data.sideout;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class SideoutMain {
    public static void main(String[] args) throws Exception {
        final Configuration config = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);
        env.disableOperatorChaining();

        DataStream<Tuple4<Integer, Long, Double, Long>> orderInfods  = env.addSource(new DataGeneratorSource<Tuple4<Integer, Long, Double, Long>>(
                new RandomGenerator<Tuple4<Integer, Long, Double, Long>>() {
            @Override
            public Tuple4 next() {
                return Tuple4.of(random.nextInt(1, 100000),
                        random.nextLong(1, 100000),
                        random.nextUniform(1, 1000),
                        System.currentTimeMillis());
            }
        }, 2, 1000L)).returns(Types.TUPLE(Types.INT, Types.LONG, Types.DOUBLE, Types.LONG));

        //orderInfods.shuffle();
        // orderInfods.print();
        // 简化化API
//        orderInfods.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<Tuple4<Integer, Long, Double, Long>>() {
//            @Override
//            public WatermarkGenerator<Tuple4<Integer, Long, Double, Long>> createWatermarkGenerator(Context context) {
//                return null;
//            }
//        }).withTimestampAssigner(new TimestampAssignerSupplier<Tuple4<Integer, Long, Double, Long>>() {
//            @Override
//            public TimestampAssigner<Tuple4<Integer, Long, Double, Long>> createTimestampAssigner(Context context) {
//                return null;
//            }
//        }).withIdleness(Duration.ofMinutes(1)));


        //
        OutputTag<Tuple4<Integer, Long, Double, Long>> lateDataOutput =
                new OutputTag("lateDataSideOut", TypeInformation.of(new TypeHint<Tuple4<Integer, Long, Double, Long>>() {
        })){};
        DataStream<Tuple4<Integer, Long, Double, Long>> orders = orderInfods.rebalance();
        SingleOutputStreamOperator<Tuple4<Integer, Long, Double, Long>> out = orders
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple4<Integer, Long, Double, Long>>() {
            @Override
            public WatermarkGenerator createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                SimpleDateFormat TS_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                return new WatermarkGenerator<Tuple4<Integer, Long, Double, Long>>() {
                    @Override
                    public void onEvent(Tuple4<Integer, Long, Double, Long> o, long l, WatermarkOutput watermarkOutput) {
                        // l，当前时间
                        //
                        watermarkOutput.emitWatermark(new Watermark(o.f3));

                        System.out.println("EventTime: " + TS_FORMATTER.format(new Date(o.f3)) + " , " +
                                "CurrentTIme: " + TS_FORMATTER.format(new Date(l)));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

                    }
                };
            }

            @Override
            public TimestampAssigner<Tuple4<Integer, Long, Double, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new TimestampAssigner<Tuple4<Integer, Long, Double, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<Integer, Long, Double, Long> event, long l) {
                        return event.f3;
                    }
                };
            }
        }.withIdleness(Duration.ofMinutes(1)));


//        out.windowAll(TumblingEventTimeWindows.of(Time.seconds(4))).process(
//                new ProcessAllWindowFunction<Tuple4<Integer, Long, Double, Long>, Object, TimeWindow>() {
//                    @Override
//                    public void process(ProcessAllWindowFunction<Tuple4<Integer, Long, Double, Long>, Object, TimeWindow>.Context context,
//                                        Iterable<Tuple4<Integer, Long, Double, Long>> iterable,
//                                        Collector<Object> collector) throws Exception {
//                        System.out.println(iterable.iterator().next());
//                    }
//                });

//        out.process(new ProcessFunction<Tuple4<Integer, Long, Double, Long>, Object>() {
//            @Override
//            public void processElement(Tuple4<Integer, Long, Double, Long> event,
//                                       ProcessFunction<Tuple4<Integer, Long, Double, Long>, Object>.Context context,
//                                       Collector<Object> collector) throws Exception {
//                context.timestamp();
//            }
//        });

        DataStream resultStream = out.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(lateDataOutput)
                .apply(new AllWindowFunction<Tuple4<Integer, Long, Double, Long>, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple4<Integer, Long, Double, Long>> input,
                                      Collector<Object> collector) throws Exception {
                        input.forEach(e -> collector.collect(e));
                        System.out.println("========================== end by: " + timeWindow.getEnd());
                    }
                });

        out.getSideOutput(lateDataOutput).writeAsText("");

        env.execute();
    }
}
