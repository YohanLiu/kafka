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

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

class TimerTaskList implements Delayed {
    private final Time time;
    // 标识当前这个链表中的总定时任务数
    private final AtomicInteger taskCounter;
    // 表示这个链表所在 Bucket 的过期时间戳
    private final AtomicLong expiration;

    // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
    // root.next points to the head
    // root.prev points to the tail
    private final TimerTaskEntry root;

    TimerTaskList(
        AtomicInteger taskCounter
    ) {
        this(taskCounter, Time.SYSTEM);
    }

    TimerTaskList(
        AtomicInteger taskCounter,
        Time time
    ) {
        this.time = time;
        this.taskCounter = taskCounter;
        this.expiration = new AtomicLong(-1L);
        this.root = new TimerTaskEntry(null, -1L);
        this.root.next = root;
        this.root.prev = root;
    }

    public boolean setExpiration(long expirationMs) {
        // 随着时钟不断向前推进，原有 Bucket 会不断地过期，然后失效。
        // 当这些 Bucket 失效后，源码会重用这些 Bucket。重用的方式就是重新设置 Bucket 的过期时间，并把它们加回到 DelayQueue 中。
        // 这里进行比较的目的，就是用来判断这个 Bucket 是否要被插入到 DelayQueue
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    public long getExpiration() {
        return expiration.get();
    }

    public synchronized void foreach(Consumer<TimerTask> f) {
        TimerTaskEntry entry = root.next;
        while (entry != root) {
            TimerTaskEntry nextEntry = entry.next;
            if (!entry.cancelled()) f.accept(entry.timerTask);
            entry = nextEntry;
        }
    }

    // 将给定定时任务插入到链表
    public void add(TimerTaskEntry timerTaskEntry) {
        boolean done = false;
        while (!done) {
            // Remove the timer task entry if it is already in any other list
            // We do this outside of the sync block below to avoid deadlocking.
            // We may retry until timerTaskEntry.list becomes null.
            // 在添加之前尝试移除该定时任务，保证该任务没有在其他链表中
            timerTaskEntry.remove();

            synchronized (this) {
                synchronized (timerTaskEntry) {
                    if (timerTaskEntry.list == null) {
                        // put the timer task entry to the end of the list. (root.prev points to the tail entry)
                        TimerTaskEntry tail = root.prev;
                        timerTaskEntry.next = root;
                        timerTaskEntry.prev = tail;
                        timerTaskEntry.list = this;
                        // 把timerTaskEntry添加到链表末尾
                        tail.next = timerTaskEntry;
                        root.prev = timerTaskEntry;
                        taskCounter.incrementAndGet();
                        done = true;
                    }
                }
            }
        }
    }

    // 从链表中移除定时任务
    public synchronized void remove(TimerTaskEntry timerTaskEntry) {
        synchronized (timerTaskEntry) {
            if (timerTaskEntry.list == this) {
                timerTaskEntry.next.prev = timerTaskEntry.prev;
                timerTaskEntry.prev.next = timerTaskEntry.next;
                timerTaskEntry.next = null;
                timerTaskEntry.prev = null;
                timerTaskEntry.list = null;
                taskCounter.decrementAndGet();
            }
        }
    }

    // flush 方法是清空链表中的所有元素，并对每个元素执行指定的逻辑。
    // 该方法用于将高层次时间轮 Bucket 上的定时任务重新插入回低层次的 Bucket 中
    public synchronized void flush(Consumer<TimerTaskEntry> f) {
        // 找到链表第一个元素
        // 第一个节点是哨兵节点，所以它的下一个元素才是真正的第一个元素
        TimerTaskEntry head = root.next;
        // 开始遍历链表
        while (head != root) {
            // 移除遍历到的链表元素
            remove(head);
            // 执行传入参数f的逻辑
            f.accept(head);
            head = root.next;
        }
        // 清空过期时间设置
        expiration.set(-1L);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(Math.max(getExpiration() - time.hiResClockMs(), 0), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        TimerTaskList other = (TimerTaskList) o;
        return Long.compare(getExpiration(), other.getExpiration());
    }
}
