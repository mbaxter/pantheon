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
package tech.pegasys.pantheon.consensus.ibft;

import static java.util.Collections.newSetFromMap;

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.p2p.wire.MessageData;

import java.util.Set;

public class MessageTracker {
  private final Set<Hash> seenMessages;

  public MessageTracker(final int messageTrackingLimit) {
    this.seenMessages = newSetFromMap(new SizeLimitedMap<>(messageTrackingLimit));
  }

  public void addSeenMessage(final MessageData message) {
    final Hash uniqueID = Hash.hash(message.getData());
    seenMessages.add(uniqueID);
  }

  public boolean hasSeenMessage(final MessageData message) {
    final Hash uniqueID = Hash.hash(message.getData());
    return seenMessages.contains(uniqueID);
  }
}
