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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

public class TaskBag<T> implements Closeable {
  private static final int DEFAULT_CACHE_SIZE = 1_000_000;
  private final int maxCacheSize;

  // The underlying queue
  private final TaskQueue<T> taskQueue;
  // A cache of tasks to operate on before going to taskQueue
  private final Queue<Task<T>> taskCache = new ArrayDeque<>();
  // Tasks that have been removed, but not marked completed yet
  private final Set<Task<T>> outstandingTasks = new HashSet<>();

  private boolean closed = false;

  public TaskBag(final TaskQueue<T> queue, final int maxCacheSize) {
    this.taskQueue = queue;
    this.maxCacheSize = maxCacheSize;
  }

  public TaskBag(final TaskQueue<T> queue) {
    this(queue, DEFAULT_CACHE_SIZE);
  }

  public synchronized void add(final T taskData) {
    assertNotClosed();
    if (cacheSize() >= maxCacheSize) {
      // Too many tasks in the cache, push this to the underlying queue
      taskQueue.enqueue(taskData);
      return;
    }

    Task<T> newTask = new CachedTask<>(this, taskData);
    taskCache.add(newTask);
  }

  public synchronized Task<T> get() {
    assertNotClosed();
    if (taskCache.size() == 0) {
      return taskQueue.dequeue();
    }

    final Task<T> pendingTask = taskCache.remove();
    outstandingTasks.add(pendingTask);
    return pendingTask;
  }

  public synchronized void clear() {
    assertNotClosed();
    taskQueue.clear();
    outstandingTasks.clear();
    taskCache.clear();
  }

  public long size() {
    return taskQueue.size() + taskCache.size();
  }

  public int cacheSize() {
    return outstandingTasks.size() + taskCache.size();
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  /** @return True if all tasks have been removed and processed. */
  public boolean allTasksCompleted() {
    return cacheSize() == 0 && taskQueue.allTasksCompleted();
  }

  private synchronized boolean completePendingTask(final CachedTask<T> cachedTask) {
    return outstandingTasks.remove(cachedTask);
  }

  private synchronized void failPendingTask(final CachedTask<T> cachedTask) {
    if (completePendingTask(cachedTask)) {
      taskCache.add(cachedTask);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
    clear();
    taskQueue.close();
  }

  private void assertNotClosed() {
    if (closed) {
      throw new IllegalStateException("Attempt to access closed " + getClass().getSimpleName());
    }
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
