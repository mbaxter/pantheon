/*
 * Copyright 2018 ConsenSys AG.
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

/**
 * Represents a very large thread-safe task queue that may exceed memory limits.
 *
 * @param <T> the type of data held in the queue
 */
public interface TaskQueue<T> extends Closeable {

  /**
   * Enqueue some data for processing.
   *
   * @param taskData The data to be processed.
   */
  void enqueue(T taskData);

  /**
   * Dequeue a task for processing. This task will be tracked as a pending task until either {@code
   * Task.markCompleted} or {@code Task.requeue} is called.
   *
   * @return The task to be processed.
   */
  Task<T> dequeue();

  /** @return The number of tasks queued for processing. */
  long queuedTasksCount();

  /** @return The number of tasks that have been dequeued, but not yet marked as completed. */
  long pendingTasksCount();

  /** @return The total number of tasks queued or pending completion. */
  default long size() {
    return queuedTasksCount() + pendingTasksCount();
  }

  /** @return True if all tasks have been processed. */
  default boolean isEmpty() {
    return size() == 0;
  }

  interface Task<T> {
    T getData();

    void markCompleted();

    void requeue();
  }
}
