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
package tech.pegasys.pantheon.ethereum.p2p.wire.messages;

import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;

import io.netty.buffer.ByteBuf;

/** A message without a body. */
abstract class EmptyMessage implements MessageData {

  @Override
  public final int getSize() {
    return 0;
  }

  @Override
  public final void writeTo(final ByteBuf output) {}

  @Override
  public final void release() {}

  @Override
  public final void retain() {}
}
