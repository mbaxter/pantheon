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
import java.util.List;

/**
 * Represents a very large thread-safe queue that may exceed memory limits.
 *
 * @param <T> the type of data held in the queue
 */
public interface BigQueue<T> extends Closeable {

  /**
   * Enqueues an element.
   *
   * @param value The element to add to the queue.
   */
  void enqueue(final T value);

  /**
   * Dequeue the next element.
   *
   * @return The next element or null if the queue is empty.
   */
  T dequeue();

  /**
   * Dequeue {@code count} elements.
   *
   * @param count The number of elements to dequeue.
   * @return A list of at most {@code count} elements, with the element at index 0 corresponding to
   *     the first dequeue operation.
   */
  List<T> dequeue(final int count);

  /**
   * Peek at the next element in the queue. Returns the next element (or null if the queue is
   * empty), but does not remove the element from the queue.
   *
   * @return The next element or null if the queue is empty.
   */
  T peek();

  /**
   * Peek at the next {@code count} elements. Will return a list containing up to {@code count}
   * elements, but does not remove these elements from the queue.
   *
   * @param count The number of elements to return.
   * @return A list of at most {@code count} elements, with the element at index 0 corresponding to
   *     the next element that would be returned from a {@code dequeue} operation.
   */
  List<T> peek(final int count);

  /** @return The number of elements in the queue. */
  long size();

  /** @return True if the queue is empty. */
  default boolean isEmpty() {
    return size() == 0;
  }
}
