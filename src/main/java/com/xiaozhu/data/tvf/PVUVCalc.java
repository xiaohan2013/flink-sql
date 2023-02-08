package com.xiaozhu.data.tvf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 用flink sql 写3h, 7h, 1d内的pv, uv
 *
 * +I[10:45:00, 10:45:20, 20000, 19900]   #  20 s
 * +I[10:45:10, 10:45:20, 20000, 19913]   #  10 s
 * +I[10:45:00, 10:45:30, 120000, 116420] #  30 s
 * +I[10:45:20, 10:45:30, 100000, 97497]
 * +I[10:45:30, 10:45:40, 100000, 97558]
 * +I[10:45:20, 10:45:40, 200000, 190314]
 *
 */
public class PVUVCalc {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        TableResult result1 = tableEnv.executeSql("create table if not exists datagen_source (" +
                "id int, " +
                "name string, " +
                "sex string, " +
                "age int, " +
                "birthday string, " +
                "proc_time as proctime()" +
                ") with (" +
                "'connector'='datagen', " +
                "'rows-per-second'='10', " +
                //"'number-of-rows' = '5', " + // 总记录数
                "'fields.id.kind'='random', " +
                "'fields.id.min'='1', " +
                "'fields.id.max'='10' " +
                ")");

        tableEnv.executeSql("create table if not exists print_sink(" +
                "start_time string, " +
                "end_time string, " +
                "pv bigint, " +
                "uv bigint " +
                ") with (" +
                "'connector'='print'" +
                ")");

        tableEnv.executeSql("insert into print_sink " +
                "select " +
                "date_format(window_start, 'HH:mm:ss'), " +
                "date_format(window_end, 'HH:mm:ss'), " +
                "count(id)," +
                "count(distinct id) " +
                "FROM TABLE(" +
                "   TUMBLE(TABLE datagen_source, DESCRIPTOR(proc_time), INTERVAL '10' SECOND)) " +
                "GROUP BY window_start, window_end " +
                "UNION ALL " +
                "select date_format(window_start, 'HH:mm:ss')," +
                "date_format(window_end, 'HH:mm:ss'), " +
                "count(id), " +
                "count(distinct id), " +
                "FROM TABLE(" +
                "   TUMBLE(TABLE datagen_source, DESCRIPTOR(proc_time), INTERVAL '20' SECOND )" +
                ") GROUP BY window_start, window_end " +
                "UNION ALL " +
                "select date_format(window_start, 'HH:mm:ss')," +
                "date_format(window_end, 'HH:mm:ss'), " +
                "count(id), " +
                "count(distinct id), " +
                "FROM TABLE(" +
                "   TUMBLE(TABLE datagen_source, DESCRIPTOR(proc_time), INTERVAL '30' SECOND )" +
                ") GROUP BY window_start, window_end " +
                "");

        env.execute();
    }
}
