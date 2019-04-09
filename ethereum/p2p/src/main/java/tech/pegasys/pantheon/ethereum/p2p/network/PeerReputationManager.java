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
package tech.pegasys.pantheon.ethereum.p2p.network;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissionsBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.util.bytes.BytesValue;

public class PeerReputationManager implements DisconnectCallback {
  private static final Set<DisconnectReason> locallyTriggeredDisconnectReasons =
      ImmutableSet.of(
          DisconnectReason.BREACH_OF_PROTOCOL, DisconnectReason.INCOMPATIBLE_P2P_PROTOCOL_VERSION);

  private static final Set<DisconnectReason> remotelyTriggeredDisconnectReasons =
      ImmutableSet.of(DisconnectReason.INCOMPATIBLE_P2P_PROTOCOL_VERSION);

  private final PeerPermissionsBlacklist peerPermissions;

  public PeerReputationManager(PeerPermissionsBlacklist peerPermissions) {
    this.peerPermissions = peerPermissions;
  }

  @Override
  public void onDisconnect(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    if (shouldBlock(reason, initiatedByPeer)) {
      peerPermissions.add(connection.getPeer());
    }
  }

  private boolean shouldBlock(
      final DisconnectReason reason, final boolean initiatedByPeer) {
    return (!initiatedByPeer && locallyTriggeredDisconnectReasons.contains(reason))
        || (initiatedByPeer && remotelyTriggeredDisconnectReasons.contains(reason));
  }
}
