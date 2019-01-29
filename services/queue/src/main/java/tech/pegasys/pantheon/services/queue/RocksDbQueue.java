package tech.pegasys.pantheon.services.queue;

import com.google.common.primitives.Longs;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.InvalidConfigurationException;
import tech.pegasys.pantheon.util.bytes.BytesValue;

public class RocksDbQueue implements BigQueue<BytesValue> {

  private static final Logger LOG = LogManager.getLogger();

  private final Options options;
  private final RocksDB db;
  private final ReadOptions iteratorOptions;
  private final RocksIterator iterator;
  private final AtomicLong lastEnqueuedKey = new AtomicLong(0);
  private final AtomicLong lastDequeuedKey = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private static void loadNativeLibrary() {
    try {
      RocksDB.loadLibrary();
    } catch (final ExceptionInInitializerError e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        LOG.info("Unable to load RocksDB library", e);
        throw new InvalidConfigurationException(
          "Unsupported platform detected. On Windows, ensure you have 64bit Java installed.");
      } else {
        throw e;
      }
    }
  }

  private RocksDbQueue(final Path storageDirectory, final MetricsSystem metricsSystem) {
    try {
      loadNativeLibrary();
      options = new Options()
        .setCreateIfMissing(true)
        // TODO: Support restoration from a previously persisted queue
        .setErrorIfExists(true);
      db = RocksDB.open(options, storageDirectory.toString());

      iteratorOptions = new ReadOptions()
        .setTailing(true);
      iterator = db.newIterator(iteratorOptions);
      iterator.seekToFirst();
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public static RocksDbQueue create(final Path storageDirectory, final MetricsSystem metricsSystem) {
    return new RocksDbQueue(storageDirectory, metricsSystem);
  }

  @Override
  public synchronized void enqueue(BytesValue value) {
    assertNotClosed();
    byte[] key = Longs.toByteArray(lastEnqueuedKey.incrementAndGet());
    try {
      db.put(key, value.getArrayUnsafe());
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public synchronized BytesValue dequeue() {
    assertNotClosed();
    if (size() == 0) {
      return null;
    }
    try {
      iterator.status();
    } catch (final RocksDBException e) {
      LOG.error("RocksDB encountered a problem while iterating.", e);
    }
    if (!iterator.isValid()) {
      return null;
    }

    // Get next value
    final byte[] keyBytes = iterator.key();
    final long key = Longs.fromByteArray(iterator.key());
    final BytesValue value = BytesValue.wrap(iterator.value());
    iterator.next();

    boolean nextIsSequential = lastDequeuedKey.compareAndSet(key - 1L, key);
    if (!nextIsSequential) {
      // The implementation has a bug if we get here.
      throw new IllegalStateException("Non-sequential dequeue detected");
    }
    try {
      db.delete(keyBytes);
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
    return value;
  }

  @Override
  public synchronized long size() {
    assertNotClosed();
    return lastEnqueuedKey.get() - lastDequeuedKey.get();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      iteratorOptions.close();
      iterator.close();
      options.close();
      db.close();
    }
  }

  private void assertNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException("Attempt to access closed " + RocksDbQueue.class.getSimpleName());
    }
  }

  public static class StorageException extends RuntimeException {
    StorageException(final Throwable t) {
      super(t);
    }
  }
}
