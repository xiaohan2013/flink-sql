package com.xiaozhu.data.udf;

import org.apache.flink.table.annotation.DataTypeHint;

import java.math.BigDecimal;


public class User {
    public int age;
    public String name;

    // 2. 复杂类型，用户可以通过 @DataTypeHint("DECIMAL(10, 2)") 注解标注此字段的数据类型
    public @DataTypeHint("DECIMAL(10, 2)") BigDecimal totalBalance;

//    public @DataTypeHint("RAW") Class<?> modelClass;
}
