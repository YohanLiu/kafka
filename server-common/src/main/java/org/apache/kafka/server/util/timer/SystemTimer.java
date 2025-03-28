/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.util.timer;

import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 它是一个定时器类，封装了分层时间轮对象，为 Purgatory 提供延迟请求管理功能。
 * 所谓的 Purgatory，就是保存延迟请求的缓冲区。也就是说，它保存的是因为不满足条件而无法完成，但是又没有超时的请求
 */
public class SystemTimer implements Timer {
    public static final String SYSTEM_TIMER_THREAD_PREFIX = "executor-";

    // timeout timer
    private final ExecutorService taskExecutor;
    private final DelayQueue<TimerTaskList> delayQueue;
    private final AtomicInteger taskCounter;
    private final TimingWheel timingWheel;

    // Locks used to protect data structures while ticking
    // 维护线程安全的读写锁
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    /**
     * @param executorName Purgatory 的名字。Kafka 中存在不同的 Purgatory，比如专门处理生产者延迟请求的 Produce 缓冲区、处理消费者延迟请求的 Fetch 缓冲区等。这里的 Produce 和 Fetch 就是 executorName
     */
    public SystemTimer(String executorName) {
        this(executorName, 1, 20, Time.SYSTEM.hiResClockMs());
    }

    /**
     *
     * @param executorName Purgatory 的名字。Kafka 中存在不同的 Purgatory，比如专门处理生产者延迟请求的 Produce 缓冲区、处理消费者延迟请求的 Fetch 缓冲区等。这里的 Produce 和 Fetch 就是 executorName
     * @param tickMs 每层时间轮向前推进的时长,单位毫秒
     * @param wheelSize 每一层时间轮上的 Bucket 数量
     * @param startMs 该 SystemTimer 定时器启动时间，单位是毫秒
     */
    public SystemTimer(
        String executorName,
        long tickMs,
        int wheelSize,
        long startMs
    ) {
        // 单线程的线程池用于异步执行定时任务
        this.taskExecutor = Executors.newFixedThreadPool(1,
            runnable -> KafkaThread.nonDaemon(SYSTEM_TIMER_THREAD_PREFIX + executorName, runnable));
        // 延迟队列保存所有Bucket，即所有TimerTaskList对象
        // 因为是 DelayQueue，所以只有在 Bucket 过期后，才能从该队列中获取到。SystemTimer 类的 advanceClock 方法正是依靠了这个特性向前驱动时钟
        this.delayQueue = new DelayQueue<>();
        // 总定时任务数
        this.taskCounter = new AtomicInteger(0);
        // 时间轮对象
        this.timingWheel = new TimingWheel(
            tickMs,
            wheelSize,
            startMs,
            taskCounter,
            delayQueue
        );
    }

    /**
     * 真正对外提供服务的.
     * 将给定的定时任务插入到时间轮中进行管理.
     * @param timerTask the task to add
     */
    public void add(TimerTask timerTask) {
        // 获取读锁。在没有线程持有写锁的前提下，
        // 多个线程能够同时向时间轮添加定时任务
        readLock.lock();
        try {
            // 调用addTimerTaskEntry执行插入逻辑
            addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs()));
        } finally {
            // 释放读锁
            readLock.unlock();
        }
    }

    /**
     * 将给定的 TimerTaskEntry 插入到时间轮中
     * @param timerTaskEntry
     */
    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        // 视timerTaskEntry状态决定执行什么逻辑：
        // 1. 未过期未取消：添加到时间轮
        // 2. 已取消：什么都不做
        // 3. 已过期：提交到线程池，等待执行
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            // 定时任务未取消，说明定时任务已过期
            // 否则timingWheel.add方法应该返回True
            if (!timerTaskEntry.cancelled()) {
                // 如果该 TimerTaskEntry 表征的定时任务没有过期或被取消，会将已经过期的定时任务提交给线程池，等待异步执行该定时任务
                taskExecutor.submit(timerTaskEntry.timerTask);
            }
        }
    }

    /**
     * 真正对外提供服务的
     * 驱动时钟向前推进
     * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
     * waits up to timeoutMs before giving up.
     */
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        // 获取delayQueue中下一个已过期的Bucket
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (bucket != null) {
            // 获取写锁
            // 一旦有线程持有写锁，其他任何线程执行add或advanceClock方法时会阻塞
            writeLock.lock();
            try {
                while (bucket != null) {
                    // 推动时间轮向前"滚动"到Bucket的过期时间点
                    timingWheel.advanceClock(bucket.getExpiration());
                    // 将该Bucket下的所有定时任务重写回到时间轮
                    bucket.flush(this::addTimerTaskEntry);
                    // 读取下一个Bucket对象
                    bucket = delayQueue.poll();
                }
            } finally {
                // 释放写锁
                writeLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 计算的是给定 Purgatory 下的总延迟请求数
     * @return
     */
    public int size() {
        return taskCounter.get();
    }

    /**
     * 关闭线程池
     */
    @Override
    public void close() {
        ThreadUtils.shutdownExecutorServiceQuietly(taskExecutor, 5, TimeUnit.SECONDS);
    }

    // visible for testing
    boolean isTerminated() {
        return taskExecutor.isTerminated();
    }
}
