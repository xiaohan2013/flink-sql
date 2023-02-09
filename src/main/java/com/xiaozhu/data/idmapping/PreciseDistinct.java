package com.xiaozhu.data.idmapping;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

//按照维度来计算
public class PreciseDistinct extends AggregateFunction<Long, PreciseAccumulator> {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        // streamTableEnvironment.createFunction("PreciseDistinct", PreciseDistinct.class);
        streamTableEnvironment.createTemporarySystemFunction("PreciseDistinct", PreciseDistinct.class);
        streamTableEnvironment.executeSql("select PreciseDistinct(order_id), price from (VALUES (1, 2.0), (2, 3.1))  AS t(order_id, price)").print();

    }

    @Override
    public PreciseAccumulator createAccumulator() {
        return new PreciseAccumulator();
    }

    @Override
    public Long getValue(PreciseAccumulator preciseAccumulator) {
        return preciseAccumulator.getCardinality();
    }

    public void accumulate(PreciseAccumulator accumulator,long id){
        accumulator.add(id);
    }
}
