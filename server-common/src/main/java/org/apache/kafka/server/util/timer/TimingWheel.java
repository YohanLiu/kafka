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

import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hierarchical Timing Wheels
 * <br>
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), &hellip;,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 * <br>
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 * <br>
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 * <pre>
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 * </pre>
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 * <br>
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 * <pre>
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 * </pre>
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 * <pre>
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 * </pre>
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 * <pre>
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 * </pre>
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 * <br>
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */
public class TimingWheel {
    // 滴答一次的时长，类似于手表的例子中向前推进一格的时间。
    // 对于秒针而言，tickMs 就是 1 秒。同理，分针是 1 分，时针是 1 小时。
    // 在 Kafka 中，第 1 层时间轮的 tickMs 被固定为 1 毫秒，也就是说，向前推进一格 Bucket 的时长是 1 毫秒
    private final long tickMs;
    // 每一层时间轮上的 Bucket 数量。第 1 层的 Bucket 数量是 20
    private final int wheelSize;
    // 这一层时间轮上的总定时任务数
    private final AtomicInteger taskCounter;
    // 将所有 Bucket 按照过期时间排序的延迟队列。随着时间不断向前推进，Kafka 需要依靠这个队列获取那些已过期的 Bucket，并清除它们
    private final DelayQueue<TimerTaskList> queue;
    // 这层时间轮总时长，等于滴答时长乘以 wheelSize。以第 1 层为例，interval 就是 20 毫秒。
    // 由于下一层时间轮的滴答时长就是上一层的总时长，因此，第 2 层的滴答时长就是 20 毫秒，总时长是 400 毫秒，以此类推
    private final long interval;
    // 时间轮下的所有 Bucket 对象，也就是所有 TimerTaskList 对象
    private final TimerTaskList[] buckets;
    // 当前时间戳，只是源码对它进行了一些微调整，将它设置成小于当前时间的最大滴答时长的整数倍。
    // 举个例子，假设滴答时长是 20 毫秒，当前时间戳是 123 毫秒，那么，currentTime 会被调整为 120 毫秒
    private long currentTimeMs;

    // overflowWheel can potentially be updated and read by two concurrent threads through add().
    // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
    // 上层时间轮的引用
    // Kafka 是按需创建上层时间轮的。这也就是说，当有新的定时任务到达时，会尝试将其放入第 1 层时间轮。如果第 1 层的 interval 无法容纳定时任务的超时时间，就现场创建并配置好第 2
    // 层时间轮，并再次尝试放入，如果依然无法容纳，那么，就再创建和配置第 3 层时间轮，以此类推，直到找到适合容纳该定时任务的第 N 层时间轮
    private volatile TimingWheel overflowWheel = null;

    TimingWheel(
        long tickMs,
        int wheelSize,
        // 时间轮对象被创建时的起始时间戳
        long startMs,
        AtomicInteger taskCounter,
        DelayQueue<TimerTaskList> queue
    ) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.buckets = new TimerTaskList[wheelSize];
        this.interval = tickMs * wheelSize;
        // rounding down to multiple of tickMs
        this.currentTimeMs = startMs - (startMs % tickMs);

        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new TimerTaskList(taskCounter);
        }
    }

    private synchronized void addOverflowWheel() {
        // 只有之前没有创建上层时间轮方法才会继续
        if (overflowWheel == null) {
            // 创建新的TimingWheel实例
            // 滴答时长tickMs等于下层时间轮总时长
            // 每层的轮子数都是相同的
            overflowWheel = new TimingWheel(
                interval,
                wheelSize,
                currentTimeMs,
                taskCounter,
                queue
            );
        }
    }

    public boolean add(TimerTaskEntry timerTaskEntry) {
        // 获取定时任务的过期时间戳
        long expiration = timerTaskEntry.expirationMs;

        // 如果该任务已然被取消了，则无需添加，直接返回
        if (timerTaskEntry.cancelled()) {
            // Cancelled
            return false;

            // 如果该任务超时时间已过期
        } else if (expiration < currentTimeMs + tickMs) {
            // Already expired
            return false;

            // 如果该任务超时时间在本层时间轮覆盖时间范围内
        } else if (expiration < currentTimeMs + interval) {
            // Put in its own bucket
            long virtualId = expiration / tickMs;
            // 计算要被放入到哪个Bucket中
            int bucketId = (int) (virtualId % (long) wheelSize);
            TimerTaskList bucket = buckets[bucketId];
            // 添加到Bucket中
            bucket.add(timerTaskEntry);

            // Set the bucket expiration time
            // 设置Bucket过期时间
            // 如果该时间变更过，说明Bucket是新建或被重用，将其加回到DelayQueue
            if (bucket.setExpiration(virtualId * tickMs)) {
                // The bucket needs to be enqueued because it was an expired bucket
                // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
                // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
                // will pass in the same value and hence return false, thus the bucket with the same expiration will not
                // be enqueued multiple times.
                queue.offer(bucket);
            }

            return true;
            // 本层时间轮无法容纳该任务，交由上层时间轮处理
        } else {
            // Out of the interval. Put it into the parent timer
            // 按需创建上层时间轮
            if (overflowWheel == null) addOverflowWheel();
            // 加入到上层时间轮中
            return overflowWheel.add(timerTaskEntry);
        }
    }

    /**
     * 向前驱动时钟的方法.
     *
     * @param timeMs 表示要把时钟向前推动到这个时点
     */
    public void advanceClock(long timeMs) {
        // 向前驱动到的时点要超过Bucket的时间范围，才是有意义的推进，否则什么都不做
        // 更新当前时间currentTime到下一个Bucket的起始时点
        if (timeMs >= currentTimeMs + tickMs) {
            currentTimeMs = timeMs - (timeMs % tickMs);

            // Try to advance the clock of the overflow wheel if present
            // 同时尝试为上一层时间轮做向前推进动作
            if (overflowWheel != null) overflowWheel.advanceClock(currentTimeMs);
        }
    }
}
