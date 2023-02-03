package com.xiaozhu.data.window;

import java.sql.Timestamp;

public class TimestampConverter {
    public static void main(String[] args) {
        String s = "2021-01-29 16:00:50";
        Timestamp ts = Timestamp.valueOf(s);
        System.out.println(ts.getTime());
    }
}
