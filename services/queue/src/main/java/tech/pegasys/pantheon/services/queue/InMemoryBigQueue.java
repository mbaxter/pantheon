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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

public class InMemoryBigQueue<T> implements BigQueue<T> {
  private long size = 0;
  private final Deque<T> internalQueue = new ArrayDeque<T>();

  @Override
  public void enqueue(final T value) {
    size += 1;
    internalQueue.addFirst(value);
  }

  @Override
  public T dequeue() {
    if (size == 0) {
      return null;
    }
    size -= 1;
    return internalQueue.removeLast();
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public void close() throws IOException {
    internalQueue.clear();
  }
}
