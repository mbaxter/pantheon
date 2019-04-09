package tech.pegasys.pantheon.ethereum.p2p.network.permissions;

import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissions;
import tech.pegasys.pantheon.ethereum.permissioning.NodeLocalConfigPermissioningController;

public class NodeLocalConfigPeerPermissions extends PeerPermissions {
  private final NodeLocalConfigPermissioningController wrapped;

  public NodeLocalConfigPeerPermissions(
    final NodeLocalConfigPermissioningController wrapped) {
    this.wrapped = wrapped;
    wrapped.subscribeToListUpdatedEvent((evt) -> this.dispatchUpdate());
  }

  @Override
  public boolean isPermitted(final Peer peer) {
    return wrapped.isPermitted(peer.getEnodeURL());
  }

  @Override
  public boolean isReady() {
    return true;
  }
}
