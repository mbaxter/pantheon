package tech.pegasys.pantheon.services.queue;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

public class InMemoryQueue<T> implements Queue<T> {
  private long size = 0;
  private Deque<T> internalQueue = new ArrayDeque<T>();

  @Override
  public void enqueue(T value) {
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
