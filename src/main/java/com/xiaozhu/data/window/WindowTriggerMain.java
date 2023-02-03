package com.xiaozhu.data.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
/**
 * eventTime window
 * e.g: window.size 5 seconds
 *            .type tumbling
 * 窗口大小为5s，代数表达式为[n, n+5)
 * 触发：
 *      窗口内的数据达到窗口边界(>n+5)触发,边界值并不参与改成窗口的计算
 *      如：输入
 *               a 1
 a 4
 a 5
 a 6
 a 4
 a 10
 a 20
 输出：
 (a_4,5)    //a 5  到达时触发，并将a 5 计入下个窗口，当前参与计算的窗口内容为a 1, a 4
 (a_6,11)   //a 10 到达触发，小于窗口左边界的数据将丢弃（窗口大小：[5,10)）, 窗口就算内容a 5, a 6。输入的a 4被丢弃
 (a,10)     //a 20 到达触发，窗口计算内容a 10，a 20计入下个窗口

 *  乱序延迟boundedOutOfOrderness：
 *  其会将允许设置延迟的数据（（n-dayle, n+5+dayle)）保留
 *       e.g 允许最大延迟 2s(dayle)
 *          触发：
 *                窗口内的数据达到窗口边界(>n+5)触发
 *          允许延迟的数据：
 *                >窗口左边界-dayle的保留，<窗口左边界-dayle的丢弃
 *          如：输入
 a 1
 a 5
 a 6
 a 7     // 触发，到达触发边界 size:5 + dayle:2
 a 3     // 丢弃，< 左边界 (n-1)*size-dayle: (2-1)*5-2
 a 4
 a 10
 a 12   //触发
 a 10
 a 9
 a 8
 a 22   //触发

 输出：
 w1:(a,1)
 w2:(a_6_7,18)
 w3:(a_12_10,32)
 */
public class WindowTriggerMain {
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.disableOperatorChaining();
        
        env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            Random random = new Random();
            int tp2 = 0;
            int speed = 0;
            int f1 = -1;

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
                while (true) {
                    TimeUnit.SECONDS.sleep(1);
                    tp2 = Math.abs(random.nextInt() % 7);
                    f1 = Math.abs(++speed + tp2);
                    sourceContext.collect(Tuple2.of("a", f1));
                    System.out.println("source generator :\t" + f1);
                           /* f1++;
                            ctx.collect(Tuple2.of("a", f1));
                            System.out.println("source generator:\t" + f1);*/
                }
            }

            @Override
            public void cancel() {
            }
        });

        //延迟数据触发边界测试
        SingleOutputStreamOperator<Tuple2<String, Integer>> socketTextStream = env
                .socketTextStream("10.164.29.143", 10086)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        try {
                            String[] infos = value.split("\\s+");
                            if (infos.length == 2) {
                                out.collect(Tuple2.of(infos[0], Integer.valueOf(infos[1])));
                            }
                        } catch (NumberFormatException e) {
                            System.out.println("format exception by:\t" + value);
                        }
                    }
                });

//        ReduceWindowPrint(socketTextStream, 0);
        ReduceWindowPrint(socketTextStream, 5);
        env.execute("event time process ");
    }

    private static void ReduceWindowPrint(SingleOutputStreamOperator<Tuple2<String, Integer>> dataStreams, int dayleTime) {
        dataStreams
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String,Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, t) -> e.f1*1000)
                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0+"_"+value2.f1, value2.f1+value1.f1);
                    }
                })
                .print();
    }

}
