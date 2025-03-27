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

public final class TimerTaskEntry {
    // 指定定时任务
    public final TimerTask timerTask;
    // 定时任务的过期时间
    public final long expirationMs;
    // 绑定的Bucket链表实例
    // Kafka 的延时请求可能会被其他线程从一个链表搬移到另一个链表中，
    // 因此，为了保证必要的内存可见性，代码声明 list 为 volatile
    volatile TimerTaskList list;
    // next指针
    TimerTaskEntry next;
    // prev指针
    TimerTaskEntry prev;

    // 关联给定的定时任务
    public TimerTaskEntry(
        TimerTask timerTask,
        long expirationMs
    ) {
        this.timerTask = timerTask;
        this.expirationMs = expirationMs;

        // if this timerTask is already held by an existing timer task entry,
        // setTimerTaskEntry will remove it.
        if (timerTask != null) {
            timerTask.setTimerTaskEntry(this);
        }
    }

    // 关联定时任务是否已经被取消了
    public boolean cancelled() {
        return timerTask.getTimerTaskEntry() != this;
    }

    // 从Bucket链表中移除自己
    public void remove() {
        TimerTaskList currentList = list;
        // If remove is called when another thread is moving the entry from a task entry list to another,
        // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
        // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
        while (currentList != null) {
            currentList.remove(this);
            currentList = list;
        }
    }
}
