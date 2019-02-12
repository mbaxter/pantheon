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

import java.util.ArrayList;
import java.util.List;

public class InMemoryBigQueue<T> implements BigQueue<T> {
  private final List<T> internalQueue = new ArrayList<>();

  @Override
  public synchronized void enqueue(final T value) {
    internalQueue.add(value);
  }

  @Override
  public synchronized T dequeue() {
    if (internalQueue.size() == 0) {
      return null;
    }
    return internalQueue.remove(0);
  }

  @Override
  public synchronized List<T> dequeue(final int count) {
    ArrayList<T> elements = new ArrayList<>(count);
    while (elements.size() < count) {
      T nextElement = dequeue();
      if (nextElement == null) {
        break;
      }
      elements.add(nextElement);
    }
    return elements;
  }

  @Override
  public synchronized T peek() {
    if (internalQueue.size() == 0) {
      return null;
    }
    return internalQueue.get(0);
  }

  @Override
  public synchronized List<T> peek(final int count) {
    int listSize = Math.min(count, internalQueue.size());
    return new ArrayList<>(internalQueue.subList(0, listSize));
  }

  @Override
  public synchronized long size() {
    return internalQueue.size();
  }

  @Override
  public synchronized void close() {
    internalQueue.clear();
  }
}
