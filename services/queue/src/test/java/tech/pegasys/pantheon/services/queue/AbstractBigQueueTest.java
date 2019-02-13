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

import static org.assertj.core.api.Assertions.assertThat;

import tech.pegasys.pantheon.services.queue.TaskQueue.Task;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.junit.Test;

abstract class AbstractBigQueueTest<T extends TaskQueue<BytesValue>> {

  protected abstract T createQueue() throws Exception;

  @Test
  public void enqueueAndDequeue() throws Exception {
    try (T queue = createQueue()) {
      BytesValue one = BytesValue.of(1);
      BytesValue two = BytesValue.of(2);
      BytesValue three = BytesValue.of(3);

      assertThat(queue.dequeue()).isNull();

      queue.enqueue(one);
      queue.enqueue(two);
      assertThat(queue.dequeue().getData()).isEqualTo(one);

      queue.enqueue(three);
      assertThat(queue.dequeue().getData()).isEqualTo(two);
      assertThat(queue.dequeue().getData()).isEqualTo(three);
      assertThat(queue.dequeue()).isNull();
      assertThat(queue.dequeue()).isNull();

      queue.enqueue(three);
      assertThat(queue.dequeue().getData()).isEqualTo(three);
    }
  }

  @Test
  public void isEmptyWhenAllTasksCompleted() throws Exception {
    try (T queue = createQueue()) {
      BytesValue one = BytesValue.of(1);
      BytesValue two = BytesValue.of(2);
      BytesValue three = BytesValue.of(3);

      assertThat(queue.isEmpty()).isTrue();

      queue.enqueue(one);
      assertThat(queue.isEmpty()).isFalse();
      queue.enqueue(two);
      assertThat(queue.isEmpty()).isFalse();
      queue.enqueue(three);
      assertThat(queue.isEmpty()).isFalse();

      final Task<BytesValue> taskOne = queue.dequeue();
      final Task<BytesValue> taskTwo = queue.dequeue();
      final Task<BytesValue> taskThree = queue.dequeue();

      assertThat(queue.isEmpty()).isFalse();

      taskOne.markCompleted();
      taskTwo.requeue();
      taskThree.markCompleted();
      assertThat(queue.isEmpty()).isFalse();

      final Task<BytesValue> requeued = queue.dequeue();
      assertThat(requeued).isNotNull();
      assertThat(requeued.getData()).isEqualTo(two);
      requeued.markCompleted();
      assertThat(queue.isEmpty()).isTrue();
    }
  }

  @Test
  public void handlesConcurrentQueuing() throws Exception {
    final int threadCount = 5;
    final int itemsPerThread = 1000;
    final T queue = createQueue();

    final CountDownLatch dequeueingFinished = new CountDownLatch(1);
    final CountDownLatch queuingFinished = new CountDownLatch(threadCount);

    // Start thread for reading values
    List<Task<BytesValue>> dequeued = new ArrayList<>();
    Thread reader =
        new Thread(
            () -> {
              while (queuingFinished.getCount() > 0 || !queue.isEmpty()) {
                if (!queue.isEmpty()) {
                  Task<BytesValue> value = queue.dequeue();
                  value.markCompleted();
                  dequeued.add(value);
                }
              }
              dequeueingFinished.countDown();
            });
    reader.start();

    final Function<BytesValue, Thread> queueingThreadFactory =
        (value) ->
            new Thread(
                () -> {
                  try {
                    for (int i = 0; i < itemsPerThread; i++) {
                      queue.enqueue(value);
                    }
                  } finally {
                    queuingFinished.countDown();
                  }
                });

    // Start threads to queue values
    for (int i = 0; i < threadCount; i++) {
      queueingThreadFactory.apply(BytesValue.of(i)).start();
    }

    queuingFinished.await();
    dequeueingFinished.await();

    assertThat(dequeued.size()).isEqualTo(threadCount * itemsPerThread);
    assertThat(dequeued.stream().filter(Objects::isNull).count()).isEqualTo(0);
    assertThat(queue.size()).isEqualTo(0);
  }
}
