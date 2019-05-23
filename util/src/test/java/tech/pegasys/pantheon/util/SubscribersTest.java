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
package tech.pegasys.pantheon.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.junit.Test;

public class SubscribersTest {
  private final Runnable subscriber1 = mock(Runnable.class);
  private final Runnable subscriber2 = mock(Runnable.class);
  private final Subscribers<Runnable> subscribers = new Subscribers<>();

  @Test
  public void shouldAddSubscriber() {
    subscribers.subscribe(subscriber1);

    assertThat(subscribers.getSubscriberCount()).isEqualTo(1);

    subscribers.forEach(Runnable::run);
    verify(subscriber1).run();
  }

  @Test
  public void shouldRemoveSubscriber() {
    final long id = subscribers.subscribe(subscriber1);
    subscribers.subscribe(subscriber2);
    assertThat(subscribers.unsubscribe(id)).isTrue();

    assertThat(subscribers.getSubscriberCount()).isEqualTo(1);
    subscribers.forEach(Runnable::run);
    verifyZeroInteractions(subscriber1);
    verify(subscriber2).run();
  }

  @Test
  public void shouldTrackMultipleSubscribers() {
    final Runnable subscriber3 = mock(Runnable.class);
    subscribers.subscribe(subscriber1);
    subscribers.subscribe(subscriber2);
    subscribers.subscribe(subscriber3);

    assertThat(subscribers.getSubscriberCount()).isEqualTo(3);
    subscribers.forEach(Runnable::run);
    verify(subscriber1).run();
    verify(subscriber2).run();
    verify(subscriber3).run();
  }

  @Test
  public void shouldClearSubscriber() {
    subscribers.subscribe(subscriber1);
    subscribers.subscribe(subscriber2);
    assertThat(subscribers.getSubscriberCount()).isEqualTo(2);

    subscribers.clear();

    assertThat(subscribers.getSubscriberCount()).isEqualTo(0);

    subscribers.forEach(Runnable::run);
    verifyZeroInteractions(subscriber1);
    verifyZeroInteractions(subscriber2);
  }
}
