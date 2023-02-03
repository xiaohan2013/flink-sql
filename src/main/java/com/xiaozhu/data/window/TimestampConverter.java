package com.xiaozhu.data.window;

import java.sql.Timestamp;
import java.util.Arrays;

public class TimestampConverter {
    public static void main(String[] args) {
//        String s = "2021-01-29 16:00:50";
//        Timestamp ts = Timestamp.valueOf(s);
//        System.out.println(ts.getTime());

        String[] s = "江苏,苏州,1,22.5,2021-01-29 16:00:50".split(",");
        Arrays.stream(s).forEach(System.out::println);
    }
}
