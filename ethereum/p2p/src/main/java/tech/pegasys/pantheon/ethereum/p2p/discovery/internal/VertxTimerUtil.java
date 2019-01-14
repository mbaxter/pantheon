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
package tech.pegasys.pantheon.ethereum.p2p.discovery.internal;

import io.vertx.core.Vertx;

public class VertxTimerUtil implements TimerUtil {

  private final Vertx vertx;

  public VertxTimerUtil(final Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public long setPeriodic(final long delay, final TimerHandler handler) {
    return vertx.setPeriodic(delay, (l) -> handler.handle());
  }

  @Override
  public long setTimer(final long delay, final TimerHandler handler) {
    return vertx.setTimer(delay, (l) -> handler.handle());
  }

  @Override
  public void cancelTimer(final long timerId) {
    vertx.cancelTimer(timerId);
  }
}
