package com.xiaozhu.data.window;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;

class TempRecordAggsResult{
    private String key;

    private LocalDateTime beginTime;

    private LocalDateTime endTime;

    private Double max;

    private Double min;

    private Double sum;

    private Double avg;

    private Integer counts;

    public static TempRecordAggsResult getInitResult() {
        TempRecordAggsResult result = new TempRecordAggsResult();
        result.setBeginTime(LocalDateTime.now());
        result.setEndTime(LocalDateTime.now());
        result.setMax(Double.MIN_VALUE);
        result.setMin(Double.MAX_VALUE);
        result.setSum(0.0);
        result.setAvg(0.0);
        result.setCounts(0);

        return result;
    }

    public TempRecordAggsResult() {
    }

    public TempRecordAggsResult(String key, LocalDateTime beginTime, LocalDateTime endTime, Double max, Double min, Double sum, Double avg, Integer counts) {
        this.key = key;
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.max = max;
        this.min = min;
        this.sum = sum;
        this.avg = avg;
        this.counts = counts;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public LocalDateTime getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(LocalDateTime beginTime) {
        this.beginTime = beginTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Double getAvg() {
        return avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
    }

    public Integer getCounts() {
        return counts;
    }

    public void setCounts(Integer counts) {
        this.counts = counts;
    }

    @Override
    public String toString() {
        return "TempRecordAggsResult{" +
                "key='" + key + '\'' +
                ", windowTime=[" + beginTime.format(FormatterConstant.commonDtf) +
                ", " + endTime.format(FormatterConstant.commonDtf) + ")" +
                ", max=" + max +
                ", min=" + min +
                ", sum=" + sum +
                ", avg=" + avg +
                ", counts=" + counts +
                '}';
    }
}

public class TemperatureCalcMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        env.setRestartStrategy(new RestartStrategies.FailureRateRestartStrategyConfiguration(2, Time.seconds(2)
//                , Time.seconds(1)));

        DataStreamSource<String> source = env.readTextFile(BaseConstant.TEMP_RECORD);

        SingleOutputStreamOperator<TempRecord> dataStream = source
                .flatMap(new TempRecordUtils.BeanFlatMap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TempRecord>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TempRecord>() {

                                    @Override
                                    public long extractTimestamp(TempRecord element, long recordTimestamp) {
                                        return element.getTimeEpochMilli();
                                    }
                                })
                );

        SingleOutputStreamOperator<TempRecordAggsResult> result = dataStream
                .keyBy(TempRecord::getCity)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .aggregate(new TempRecordUtils.MyAggregateFunction(),//增量计算
                        new TempRecordUtils.MyProcessWindow());//全量计算

        result.print();

        env.execute();

    }
}
