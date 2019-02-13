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

import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.metrics.OperationTimer;
import tech.pegasys.pantheon.services.util.RocksDbUtil;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.primitives.Longs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDbQueue implements BytesTaskQueue {

  private static final Logger LOG = LogManager.getLogger();

  private final Options options;
  private final RocksDB db;

  private final AtomicLong lastEnqueuedKey = new AtomicLong(0);
  private final AtomicLong lastDequeuedKey = new AtomicLong(0);
  private final AtomicLong lastDeletedKey = new AtomicLong(0);
  private final Set<RocksDBTask> outstandingTasks = new HashSet<>();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final OperationTimer enqueueLatency;
  private final OperationTimer dequeueLatency;

  private RocksDbQueue(final Path storageDirectory, final MetricsSystem metricsSystem) {
    try {
      RocksDbUtil.loadNativeLibrary();
      options =
          new Options()
              .setCreateIfMissing(true)
              // TODO: Support restoration from a previously persisted queue
              .setErrorIfExists(true);
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

  public static RocksDbQueue create(
      final Path storageDirectory, final MetricsSystem metricsSystem) {
    return new RocksDbQueue(storageDirectory, metricsSystem);
  }

  @Override
  public synchronized void enqueue(final BytesValue taskData) {
    assertNotClosed();
    try (final OperationTimer.TimingContext ignored = enqueueLatency.startTimer()) {
      byte[] key = Longs.toByteArray(lastEnqueuedKey.incrementAndGet());
      db.put(key, taskData.getArrayUnsafe());
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public synchronized Task<BytesValue> dequeue() {
    assertNotClosed();
    if (queuedTasksCount() == 0) {
      return null;
    }
    try (final OperationTimer.TimingContext ignored = dequeueLatency.startTimer()) {
      long key = lastDequeuedKey.incrementAndGet();
      byte[] value = db.get(Longs.toByteArray(key));
      if (value == null) {
        throw new IllegalStateException("Next expected value is missing");
      }

      BytesValue data = BytesValue.of(value);
      RocksDBTask task = new RocksDBTask(this, data, key);
      outstandingTasks.add(task);
      return task;
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public synchronized long queuedTasksCount() {
    assertNotClosed();
    return lastEnqueuedKey.get() - lastDequeuedKey.get();
  }

  @Override
  public synchronized long pendingTasksCount() {
    assertNotClosed();
    return outstandingTasks.size();
  }

  private synchronized void deleteCompletedTasks() {
    RocksDBTask oldestOutstandingTask =
        outstandingTasks.stream().min(Comparator.comparingLong(RocksDBTask::getKey)).orElse(null);
    if (oldestOutstandingTask == null) {
      return;
    }

    long oldestKey = oldestOutstandingTask.getKey();
    if (lastDeletedKey.get() < oldestKey) {
      // Delete all contiguous completed tasks
      byte[] fromKey = Longs.toByteArray(lastDeletedKey.get());
      byte[] toKey = Longs.toByteArray(oldestKey);
      try {
        db.deleteRange(fromKey, toKey);
        lastDeletedKey.set(oldestKey);
      } catch (RocksDBException e) {
        throw new StorageException(e);
      }
    }
  }

  @Override
  public synchronized long size() {
    assertNotClosed();
    return BytesTaskQueue.super.size();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      options.close();
      db.close();
    }
  }

  private void assertNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException(
          "Attempt to access closed " + RocksDbQueue.class.getSimpleName());
    }
  }

  private synchronized void markTaskCompleted(RocksDBTask task) {
    outstandingTasks.remove(task);
    deleteCompletedTasks();
  }

  private synchronized void requeueTask(RocksDBTask task) {
    outstandingTasks.remove(task);
    deleteCompletedTasks();
    enqueue(task.getData());
  }

  public static class StorageException extends RuntimeException {
    StorageException(final Throwable t) {
      super(t);
    }
  }

  private static class RocksDBTask implements Task<BytesValue> {
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final RocksDbQueue parentQueue;
    private final BytesValue data;
    private final long key;

    private RocksDBTask(RocksDbQueue parentQueue, BytesValue data, long key) {
      this.parentQueue = parentQueue;
      this.data = data;
      this.key = key;
    }

    public long getKey() {
      return key;
    }

    @Override
    public BytesValue getData() {
      return data;
    }

    @Override
    public void markCompleted() {
      if (completed.compareAndSet(false, true)) {
        parentQueue.markTaskCompleted(this);
      }
    }

    @Override
    public void requeue() {
      if (completed.compareAndSet(false, true)) {
        parentQueue.requeueTask(this);
      }
    }
  }
}
