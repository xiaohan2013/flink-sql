package com.xiaozhu.data.idmapping;

import org.apache.flink.table.functions.AggregateFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

//按照维度来计算
class PreciseAccumulator{
    private Roaring64NavigableMap bitmap;

    public PreciseAccumulator(){
        bitmap=new Roaring64NavigableMap();
    }

    public void add(long id){
        bitmap.addLong(id);
    }

    public long getCardinality(){
        return bitmap.getLongCardinality();
    }
}

public class PreciseDistinct extends AggregateFunction<Long, PreciseAccumulator> {

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
