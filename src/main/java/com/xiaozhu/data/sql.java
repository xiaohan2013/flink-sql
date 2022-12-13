package com.xiaozhu.data;

import com.xiaozhu.data.udf.HashCode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.TimeIntervalUnit;


import static org.apache.flink.table.api.Expressions.$;

enum RowKind {
    INSERT("+I", (byte) 0),
    UPDATE_BEFORE("-U", (byte) 1),
    UPDATE_AFTER("+U", (byte) 2),
    DELETE("-D", (byte) 3);
    private String op;
    private byte code;
    private RowKind(String op, byte code){
        this.code = code;
        this.op = op;
    }
}

public class sql {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);
//        tableEnv.getConfig().set("parallelism.default","1");

        // 创建输入源表
        tableEnv.executeSql("CREATE TABLE data_gen (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen'," +
                "    'rows-per-second' = '1'," +
                "    'number-of-rows' = '3'," +
                "    'fields.amount.kind' = 'random'," +
                "    'fields.amount.min' = '10'," +
                "    'fields.amount.max' = '11'," +
                "    'fields.account_id.kind' = 'random'," +
                "    'fields.account_id.min' = '1'," +
                "    'fields.account_id.max' = '2'" +
                ")");

        tableEnv.executeSql("CREATE TABLE print (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "   'connector'  = 'print'\n" +
                ")");

//        tableEnv.registerFunction("utc2local", new ScalarFunctionImpl());
        // register the function
        tableEnv.createTemporarySystemFunction("hashCode", new HashCode());

        Table data_gen = tableEnv.from("data_gen");
        Table report = report(data_gen);
        report.executeInsert("print");

        // 分组批量
        /**
         * 修改官方原生示例来源端为data_gen 目标端为：print 验证逻辑
         * sql操作
         * watermark 实际效果在此种 group aggregate貌似没有实际意义
         * group aggregate 是更新流，相同key的结果会不断更新输出， 没有窗口的说法，会累积更新结果
         * window aggredate 是窗口聚合，每个窗口结束时输出结果
         * 换成batch mode 查看效果，此模式要求 source要支持有界流配置
         */
        tableEnv.executeSql("insert into print select account_id ,count(amount) as count1 from data_gen group by account_id");

        // 1秒内的数据进行account_id分组聚合
        tableEnv.executeSql("insert into print select account_id, count(amount) as count1" +
                " from data_gen group by account_id, " +
                " TUMBLE(transaction_time, INTERVAL  '10' SECOND)");

        // window聚合-使用处理实践ProcTime
        /**
         * 修改官方原生示例来源端为data_gen 目标端为：print 验证逻辑
         * sql操作
         * group aggregate 是更新流，相同key的结果会不断更新输出， 没有窗口的说法，会累积更新结果
         * window aggredate 是窗口聚合，每个窗口结束时输出结果
         *
         * 使用系统时间
         * 输出window 结束时间
         */
        tableEnv.executeSql("insert into print select account_id, TUMBLE_END(transaction_time, INTERVAL '10' SECOND) as window_end," +
            "count(amount) as count1," +
                "from data_gen group by account_id," +
                "TUMBLE(transaction_time, INTERVAL, '10' SECOND)"
        );
    }

    public static Table report(Table transactions) {
        return transactions.select(
                        $("account_id"),
                        $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
                        $("amount"))
                        .groupBy($("account_id"),$("log_ts"))
                        .select(
                                $("account_id"),
                                $("log_ts"),
                                $("amount").sum().as("amount"));
    }
}
