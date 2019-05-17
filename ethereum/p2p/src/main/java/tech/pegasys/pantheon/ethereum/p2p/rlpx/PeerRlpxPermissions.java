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
package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PeerPermissions.Action;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PermissionsUpdateCallback;

import java.util.Optional;
import java.util.function.Supplier;

public class PeerRlpxPermissions {
  private final Supplier<Optional<Peer>> localNodeSupplier;
  private final PeerPermissions peerPermissions;

  public PeerRlpxPermissions(
      final Supplier<Optional<Peer>> localNodeSupplier, final PeerPermissions peerPermissions) {
    this.localNodeSupplier = localNodeSupplier;
    this.peerPermissions = peerPermissions;
  }

  public boolean allowNewOutboundConnectionTo(final Peer peer) {
    Optional<Peer> localNode = localNodeSupplier.get();
    if (!localNode.isPresent()) {
      return false;
    }
    return peerPermissions.isPermitted(
        localNode.get(), peer, Action.RLPX_ALLOW_NEW_OUTBOUND_CONNECTION);
  }

  public boolean allowNewInboundConnectionFrom(final Peer peer) {
    Optional<Peer> localNode = localNodeSupplier.get();
    if (!localNode.isPresent()) {
      return false;
    }
    return peerPermissions.isPermitted(
        localNode.get(), peer, Action.RLPX_ALLOW_NEW_INBOUND_CONNECTION);
  }

  public boolean allowOngoingConnection(final Peer peer) {
    Optional<Peer> localNode = localNodeSupplier.get();
    if (!localNode.isPresent()) {
      return false;
    }
    return peerPermissions.isPermitted(localNode.get(), peer, Action.RLPX_ALLOW_ONGOING_CONNECTION);
  }

  public void subscribeUpdate(final PermissionsUpdateCallback callback) {
    peerPermissions.subscribeUpdate(callback);
  }

  // TODO: Store origination information in peer connection
  //  public boolean allowOngoingConnectionInitiatedLocally(Peer peer) {
  //    return peerPermissions.isPermitted(localNode, peer,
  // Action.RLPX_ALLOW_ONGOING_CONNECTION_LOCALLY_INITIATED);
  //  }
  //
  //  public boolean allowOngoingConnectionInitiatedRemotely(final Peer peer) {
  //    return peerPermissions.isPermitted(localNode, peer,
  // Action.RLPX_ALLOW_ONGOING_CONNECTION_LOCALLY_INITIATED);
  //  }

}
