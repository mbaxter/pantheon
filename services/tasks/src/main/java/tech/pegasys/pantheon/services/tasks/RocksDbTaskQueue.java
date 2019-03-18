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
package tech.pegasys.pantheon.services.tasks;

import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.metrics.OperationTimer;
import tech.pegasys.pantheon.services.util.RocksDbUtil;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.primitives.Longs;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksDbTaskQueue<T> implements TaskCollection<T> {

  private final Options options;
  private final RocksDB db;

  private long lastEnqueuedKey = 0;
  private long lastDequeuedKey = 0;
  private RocksIterator dequeueIterator;
  private long lastValidKeyFromIterator;
  private final Set<RocksDbTask<T>> outstandingTasks = new HashSet<>();

  private boolean closed = false;

  private final Function<T, BytesValue> serializer;
  private final Function<BytesValue, T> deserializer;

  private final OperationTimer enqueueLatency;
  private final OperationTimer dequeueLatency;

  private RocksDbTaskQueue(
      final Path storageDirectory,
      final Function<T, BytesValue> serializer,
      final Function<BytesValue, T> deserializer,
      final MetricsSystem metricsSystem) {
    this.serializer = serializer;
    this.deserializer = deserializer;
    try {
      RocksDbUtil.loadNativeLibrary();
      // We don't support reloading data so ensure we're starting from a clean slate.
      RocksDB.destroyDB(storageDirectory.toString(), new Options());
      options = new Options().setCreateIfMissing(true).setErrorIfExists(true);
      db = RocksDB.open(options, storageDirectory.toString());

      enqueueLatency =
          metricsSystem.createTimer(
              MetricCategory.BIG_QUEUE,
              "enqueue_latency_seconds",
              "Latency for enqueuing an item.");
      dequeueLatency =
          metricsSystem.createTimer(
              MetricCategory.BIG_QUEUE,
              "dequeue_latency_seconds",
              "Latency for dequeuing an item.");

    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public static <T> RocksDbTaskQueue<T> create(
      final Path storageDirectory,
      final Function<T, BytesValue> serializer,
      final Function<BytesValue, T> deserializer,
      final MetricsSystem metricsSystem) {
    return new RocksDbTaskQueue<>(storageDirectory, serializer, deserializer, metricsSystem);
  }

  @Override
  public synchronized void add(final T taskData) {
    assertNotClosed();
    try (final OperationTimer.TimingContext ignored = enqueueLatency.startTimer()) {
      final long key = ++lastEnqueuedKey;
      db.put(Longs.toByteArray(key), serializer.apply(taskData).getArrayUnsafe());
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public synchronized Task<T> remove() {
    assertNotClosed();
    if (isEmpty()) {
      return null;
    }
    try (final OperationTimer.TimingContext ignored = dequeueLatency.startTimer()) {
      if (dequeueIterator == null) {
        createNewIterator();
      }
      final long key = ++lastDequeuedKey;
      dequeueIterator.seek(Longs.toByteArray(key));
      if (key > lastValidKeyFromIterator || !dequeueIterator.isValid()) {
        // Reached the end of the snapshot this iterator was loaded with
        dequeueIterator.close();
        createNewIterator();
        dequeueIterator.seek(Longs.toByteArray(key));
        if (!dequeueIterator.isValid()) {
          throw new IllegalStateException("Next expected value is missing");
        }
      }
      final byte[] value = dequeueIterator.value();
      final BytesValue data = BytesValue.of(value);
      final RocksDbTask<T> task = new RocksDbTask<>(this, deserializer.apply(data), key);
      outstandingTasks.add(task);
      return task;
    }
  }

  private void createNewIterator() {
    dequeueIterator = db.newIterator();
    lastValidKeyFromIterator = lastEnqueuedKey;
  }

  @Override
  public synchronized long size() {
    if (closed) {
      return 0;
    }
    return lastEnqueuedKey - lastDequeuedKey;
  }

  @Override
  public synchronized boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public synchronized void clear() {
    assertNotClosed();
    outstandingTasks.clear();
    final byte[] from = Longs.toByteArray(0);
    final byte[] to = Longs.toByteArray(lastEnqueuedKey + 1);
    try {
      db.deleteRange(from, to);
      if (dequeueIterator != null) {
        dequeueIterator.close();
        dequeueIterator = null;
      }
      lastDequeuedKey = 0;
      lastEnqueuedKey = 0;
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public synchronized boolean allTasksCompleted() {
    return isEmpty() && outstandingTasks.isEmpty();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (dequeueIterator != null) {
      dequeueIterator.close();
    }
    options.close();
    db.close();
  }

  private void assertNotClosed() {
    if (closed) {
      throw new IllegalStateException("Attempt to access closed " + getClass().getSimpleName());
    }
  }

  private synchronized boolean markTaskCompleted(final RocksDbTask<T> task) {
    return outstandingTasks.remove(task);
  }

  private synchronized void handleFailedTask(final RocksDbTask<T> task) {
    if (markTaskCompleted(task)) {
      add(task.getData());
    }
  }

  public static class StorageException extends RuntimeException {
    StorageException(final Throwable t) {
      super(t);
    }
  }

  private static class RocksDbTask<T> implements Task<T> {
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final RocksDbTaskQueue<T> parentQueue;
    private final T data;
    private final long key;

    private RocksDbTask(final RocksDbTaskQueue<T> parentQueue, final T data, final long key) {
      this.parentQueue = parentQueue;
      this.data = data;
      this.key = key;
    }

    public long getKey() {
      return key;
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public void markCompleted() {
      if (completed.compareAndSet(false, true)) {
        parentQueue.markTaskCompleted(this);
      }
    }

    @Override
    public void markFailed() {
      if (completed.compareAndSet(false, true)) {
        parentQueue.handleFailedTask(this);
      }
    }
  }
}
