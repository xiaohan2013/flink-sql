package com.xiaozhu.data.window;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.scala.function.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

@Builder
class User {
    private int id;
    private String name;
    private String sex;
    private int age;
    private String birthday;
    private Long proc_time;

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age=" + age +
                ", birthday='" + birthday + '\'' +
                ", proc_time=" + proc_time +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public Long getProc_time() {
        return proc_time;
    }

    public void setProc_time(Long proc_time) {
        this.proc_time = proc_time;
    }
}

@Builder
class PVUV {
    private String window_start;
    private String window_end;
    private Long pv;
    private Long uv;

    @Override
    public String toString() {
        return "PVUV{" +
                "window_start='" + window_start + '\'' +
                ", window_end='" + window_end + '\'' +
                ", pv=" + pv +
                ", uv=" + uv +
                '}';
    }

    public String getWindow_start() {
        return window_start;
    }

    public void setWindow_start(String window_start) {
        this.window_start = window_start;
    }

    public String getWindow_end() {
        return window_end;
    }

    public void setWindow_end(String window_end) {
        this.window_end = window_end;
    }

    public Long getPv() {
        return pv;
    }

    public void setPv(Long pv) {
        this.pv = pv;
    }

    public Long getUv() {
        return uv;
    }

    public void setUv(Long uv) {
        this.uv = uv;
    }
}

public class LongWindowMain {
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();

        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(Duration.ofSeconds(10).getSeconds(), CheckpointingMode.EXACTLY_ONCE);


        SingleOutputStreamOperator<User> source = env.addSource(new DataGeneratorSource<User>(new RandomGenerator<User>() {
            @Override
            public User next() {
                return User.builder()
                        .id(random.nextInt(1, 5))
                        .name(random.nextSecureHexString(6))
                        .sex(random.nextSample(Arrays.asList("0", "1"), 1)[0].toString())
                        .age(random.nextInt(10, 90))
                        .birthday(random.nextSecureHexString(8))
                        .proc_time(System.currentTimeMillis()).build();
            }
        }, 10, null)).returns(new TypeHint<User>() {});

        // 不按key分
        DataStream pvuv = source
                .assignTimestampsAndWatermarks(WatermarkStrategy.<User>forMonotonousTimestamps()
                        .withTimestampAssigner(new TimestampAssignerSupplier<User>() {
                    @Override
                    public TimestampAssigner<User> createTimestampAssigner(Context context) {
                        return new TimestampAssigner<User>() {
                            @Override
                            public long extractTimestamp(User user, long l) {
                                return user.getProc_time();
                            }
                        };
                    }
                }))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(2))) // 可能晚到
                .aggregate(new AggregateFunction<User, Tuple2<Long, Long>, PVUV>() {
                    private Set<Integer> users = new HashSet<>();

                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L, 0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(User user, Tuple2<Long, Long> o) {
                        o.f0 = o.f0 + 1;
                        if(!users.contains(user.getId())){
                            o.f1 = o.f1 + 1;
                            users.add(user.getId());
                        }
                        System.out.println(user.toString() + " " + o.toString());
                        return o;
                    }

                    @Override
                    public PVUV getResult(Tuple2<Long, Long> o) {
                        return PVUV.builder().pv(o.f0).uv(o.f1).build();
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> o, Tuple2<Long, Long> acc1) {
                        return null;
                    }
                }, new AllWindowFunction<PVUV, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<PVUV> iterable, Collector<Object> collector) throws Exception {
                        iterable.forEach(new Consumer<PVUV>() {
                            @Override
                            public void accept(PVUV pvuv) {
                                SimpleDateFormat formatter  = new SimpleDateFormat("yyyy-MM-dd HH:mm:dd.sss");
                                Date start_date = new Date(timeWindow.getStart());
                                Date end_date = new Date(timeWindow.getEnd());
                                pvuv.setWindow_start(formatter.format(start_date));
                                pvuv.setWindow_end(formatter.format(end_date));
                                System.out.println(pvuv.toString());
                            }
                        });
                        System.out.println("[ " + timeWindow.getStart() + " - " + timeWindow.getEnd() + " ]");
                    }
                });

        pvuv.print();

        env.execute();
    }
}
