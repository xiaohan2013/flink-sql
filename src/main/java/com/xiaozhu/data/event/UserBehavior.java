package com.xiaozhu.data.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
public class UserBehavior {
    private String id;
    private long timestamp;
}
