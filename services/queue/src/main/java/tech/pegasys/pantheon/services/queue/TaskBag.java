/*
 * Copyright 2019 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package tech.pegasys.pantheon.services.queue;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

public class TaskBag<T> {
  private final int maxCacheSize;

  private final TaskQueue<T> taskQueue;
  private final Queue<Task<T>> taskCache = new ArrayDeque<>();
  private final Set<Task<T>> pendingTasks = new HashSet<>();

  public TaskBag(final TaskQueue<T> queue, final int maxCacheSize) {
    this.taskQueue = queue;
    this.maxCacheSize = maxCacheSize;
  }

  public synchronized void add(final T taskData) {
    if (cacheSize() >= maxCacheSize) {
      // Too many tasks in the cache, push this to the underlying queue
      taskQueue.enqueue(taskData);
      return;
    }

    Task<T> newTask = new CachedTask<>(this, taskData);
    taskCache.add(newTask);
  }

  public synchronized Task<T> get() {
    if (cacheSize() == 0) {
      return taskQueue.dequeue();
    }

    if (taskCache.size() == 0) {
      return null;
    }

    final Task<T> pendingTask = taskCache.remove();
    pendingTasks.add(pendingTask);
    return pendingTask;
  }

  public long size() {
    return taskQueue.size() + cacheSize();
  }

  public void clear() {
    taskQueue.clear();
    pendingTasks.clear();
    taskCache.clear();
  }

  public int cacheSize() {
    return pendingTasks.size() + taskCache.size();
  }

  private synchronized void completePendingTask(final CachedTask<T> cachedTask) {
    pendingTasks.remove(cachedTask);
  }

  private synchronized void failPendingTask(final CachedTask<T> cachedTask) {
    pendingTasks.remove(cachedTask);
    taskCache.add(cachedTask);
  }

  private static class CachedTask<T> implements Task<T> {
    private final TaskBag<T> taskBag;
    private final T data;

    private CachedTask(final TaskBag<T> taskBag, final T data) {
      this.taskBag = taskBag;
      this.data = data;
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public void markCompleted() {
      taskBag.completePendingTask(this);
    }

    @Override
    public void markFailed() {
      taskBag.failPendingTask(this);
    }
  }
}
