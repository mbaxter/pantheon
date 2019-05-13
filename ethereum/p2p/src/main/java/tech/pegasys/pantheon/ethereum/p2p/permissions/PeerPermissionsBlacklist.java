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
package tech.pegasys.pantheon.ethereum.p2p.permissions;

import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.util.LimitedSet;
import tech.pegasys.pantheon.util.LimitedSet.Mode;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.OptionalInt;
import java.util.Set;

import io.vertx.core.impl.ConcurrentHashSet;

public class PeerPermissionsBlacklist extends PeerPermissions {
  private static int DEFAULT_INITIAL_CAPACITY = 20;

  private final Set<BytesValue> blacklist;

  private PeerPermissionsBlacklist(final int initialCapacity, final OptionalInt maxSize) {
    if (maxSize.isPresent()) {
      blacklist =
          LimitedSet.create(initialCapacity, maxSize.getAsInt(), Mode.DROP_LEAST_RECENTLY_ACCESSED);
    } else {
      blacklist = new ConcurrentHashSet<>(initialCapacity);
    }
  }

  private PeerPermissionsBlacklist(final OptionalInt maxSize) {
    this(DEFAULT_INITIAL_CAPACITY, maxSize);
  }

  public static PeerPermissionsBlacklist create() {
    return new PeerPermissionsBlacklist(OptionalInt.empty());
  }

  public static PeerPermissionsBlacklist create(final int maxSize) {
    return new PeerPermissionsBlacklist(OptionalInt.of(maxSize));
  }

  @Override
  public boolean isPermitted(final Peer peer) {
    return !blacklist.contains(peer.getId());
  }

  public void add(final Peer peer) {
    add(peer.getId());
  }

  public void remove(final Peer peer) {
    remove(peer.getId());
  }

  public void add(final BytesValue peerId) {
    if (blacklist.add(peerId)) {
      dispatchUpdate(true);
    }
  }

  public void remove(final BytesValue peerId) {
    if (blacklist.remove(peerId)) {
      dispatchUpdate(false);
    }
  }
}
