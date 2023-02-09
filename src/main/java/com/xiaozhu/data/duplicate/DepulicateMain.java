package com.xiaozhu.data.duplicate;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

class ADKey {
    private long time;
    public long getTime(){
        return this.time;
    }
}

class ADData {
    private String deviceId;
    public String getDeviceId(){
        return this.deviceId;
    }
}

// 方便起见使用输出类型使用Void,这里直接使用打印控制台方式查看结果，在实际中可输出到下游做一个批量的处理然后输出
class DistinctProcessFunction extends KeyedProcessFunction<ADKey, ADData, Void>{

    // 定义第一个MapState
    MapState<String, Integer> deviceIdState;

    // 定义第二个ValueState
    ValueState<Long> countState;

    @Override
    public void open(Configuration conf) {
        MapStateDescriptor<String, Integer> deviceIdStateDescriptor = new MapStateDescriptor<String, Integer>("deviceIdState", String.class, Integer.class);
        /**
         MapState，key表示devId, value表示一个随意的值只是为了标识，该状态表示一个广告位在某个小时的设备数据，
         如果我们使用rocksdb作为statebackend, 那么会将mapstate中key作为rocksdb中key的一部分，
         mapstate中value作为rocksdb中的value, rocksdb中value大小是有上限的，这种方式可以减少rocksdb value的大小；
         */
        deviceIdState = getRuntimeContext().getMapState(deviceIdStateDescriptor);

        ValueStateDescriptor<Long> countStateDescriptor = new ValueStateDescriptor<Long>("countState", Long.class);

        /*
          ValueState,存储当前MapState的数据量，是由于mapstate只能通过迭代方式获得数据量大小，每次获取都需要进行迭代，这种方式可以避免每次迭代。
         */
        countState = getRuntimeContext().getState(countStateDescriptor);
    }

    @Override
    public void processElement(ADData o, KeyedProcessFunction<ADKey, ADData, Void>.Context context, Collector<Void> collector) throws Exception {
        long current_watermark = context.timerService().currentWatermark();
        if(context.getCurrentKey().getTime() + 1 < current_watermark){
            System.out.println("Late date: " + o.toString());
            return;
        }

        String deviceId = o.getDeviceId();
        Integer i = deviceIdState.get(deviceId);

        if(i == null) {
            i = 0;
        } else {
            // 不存在，放入到状态
            deviceIdState.put(deviceId, 1);
            // 将统计的数据 + 1
            Long count = countState.value();

            if(count == null) {
                count = 0L;
            }
            count++;
            countState.update(count);
            // 注册一个定时器，定期清理状态中的数据
            context.timerService().registerEventTimeTimer(context.getCurrentKey().getTime() + 1);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception{
        System.out.println(timestamp + " exec clean.");
        System.out.println("countState.value = " + countState.value());
        // 清除状态
        // 定期清除
        deviceIdState.clear();
        countState.clear();
    }
}


public class DepulicateMain {
    public static void main(String[] args) {

    }
}
