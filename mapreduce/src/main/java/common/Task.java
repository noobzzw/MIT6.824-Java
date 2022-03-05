package common;/*
 * common.Task.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gusu
 * @date 2021/6/26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Task {
    // task id
    private Integer id;
    // Cons.TASK_TYPE_MAP or Cons.TASK_TYPE_REDUCE
    private Integer type;
    // 一个file开启一个task
    private TaskFile taskFile;
    // 当前时间戳，用于评估任务
    private long timestamp;
}
