package com.xiaozhu.data.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

public class TempRecordUtils {
    /**
     * 聚合函数，每来一个数据，就会执行聚合操作
     */
    public static class MyAggregateFunction implements AggregateFunction<
                TempRecord,
                TempRecordAggsResult,
                TempRecordAggsResult> {

        @Override
        public TempRecordAggsResult createAccumulator() {
            return TempRecordAggsResult.getInitResult();
        }

        /**
         * 每进入一个数据就会执行一次
         * @param value 当前进入的数据
         * @param accumulator 之前计算好的中间结果
         * @return
         */
        @Override
        public TempRecordAggsResult add(TempRecord value, TempRecordAggsResult accumulator) {

            accumulator.setKey(value.getProvince() + "," + value.getCity());
            accumulator.setMax(value.getTemp() > accumulator.getMax() ? value.getTemp() : accumulator.getMax());
            accumulator.setMin(value.getTemp() < accumulator.getMin() ? value.getTemp() : accumulator.getMin());
            accumulator.setSum(value.getTemp() + accumulator.getSum());
            accumulator.setCounts(accumulator.getCounts() + 1);
            accumulator.setAvg(accumulator.getSum() / accumulator.getCounts());
            return accumulator;
        }

        /*
        当window的结束时间到达时，触发这个方法，返回结果
         */
        @Override
        public TempRecordAggsResult getResult(TempRecordAggsResult accumulator) {
            //System.out.println("getResult :" + accumulator.toString());
            return accumulator;
        }

        /**
         * 在session窗口才会用到merge，时间窗口其实用不得
         * @param a
         * @param b
         * @return
         */
        @Override
        public TempRecordAggsResult merge(TempRecordAggsResult a, TempRecordAggsResult b) {
            a.setMax(a.getMax() > b.getMax() ? a.getMax() : b.getMax());
            a.setMin(a.getMin() < b.getMin() ? a.getMin() : b.getMin());
            a.setSum(a.getSum() + b.getSum());
            a.setCounts(a.getCounts() + b.getCounts());
            a.setAvg(a.getSum() / a.getCounts());
            return a;
        }
    }


    public static class MyProcessWindow extends ProcessWindowFunction<
                TempRecordAggsResult,
                TempRecordAggsResult,
                String,
                TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<TempRecordAggsResult> elements, Collector<TempRecordAggsResult> out) throws Exception {

            long windowStartTs = context.window().getStart();
            long windowEndTs = context.window().getEnd();

            if (elements.iterator().hasNext()) {

                TempRecordAggsResult result = elements.iterator().next();

                System.out.println("result:" + result.toString());

                result.setBeginTime(
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(windowStartTs), ZoneId.systemDefault()
                        )
                );

                result.setEndTime(
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(windowEndTs), ZoneId.systemDefault()
                        )
                );

                out.collect(result);
            }
        }
    }

    public static class BeanFlatMap implements FlatMapFunction<String, TempRecord> {
        @Override
        public void flatMap(String o, Collector<TempRecord> collector) throws Exception {
            TempRecord tempRecord = new TempRecord();
            String[] strings = o.split(",");
            tempRecord.setProvince(strings[0]);
            tempRecord.setCity(strings[1]);
            tempRecord.setTemp(Double.parseDouble(strings[3]));
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date date = formatter.parse(strings[4]);
            ((SimpleDateFormat) formatter).applyPattern("yyyy-MM-dd HH:mm:ss.SSS");
            String newDateString = formatter.format(date);
            tempRecord.setTimeEpochMilli(Timestamp.valueOf(newDateString).getTime());
            collector.collect(tempRecord);
        }
    }
}
