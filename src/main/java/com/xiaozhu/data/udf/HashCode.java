package com.xiaozhu.data.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class HashCode extends ScalarFunction {
    private int factor = 0;

    @Override
    public void open(FunctionContext context) throws Exception {
        // access "hashcode_factor" parameter
        // "12" would be the default value if parameter does not exist
        factor = Integer.valueOf(context.getJobParameter("hashcode_factor", "12"));
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }
}
