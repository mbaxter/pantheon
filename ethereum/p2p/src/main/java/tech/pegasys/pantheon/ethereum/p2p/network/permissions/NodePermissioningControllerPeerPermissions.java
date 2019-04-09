package tech.pegasys.pantheon.ethereum.p2p.network.permissions;

import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissions;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;

public class NodePermissioningControllerPeerPermissions extends PeerPermissions {
  private final NodePermissioningController wrapped;
  private final LocalNode localNode;

  public NodePermissioningControllerPeerPermissions(
    final NodePermissioningController wrapped,
    final LocalNode localNode,
    final Blockchain blockchain) {
    this.wrapped = wrapped;
    this.localNode = localNode;
    wrapped.subscribeToUpdates(this::dispatchUpdate);
    blockchain.observeBlockAdded((e, b) -> dispatchUpdate());
  }

  @Override
  public boolean isPermitted(final Peer peer) {
    if (!isReady()) {
      return false;
    }
    final Peer localPeer = localNode.getPeer().get();
    return wrapped.isPermitted(localPeer.getEnodeURL(), peer.getEnodeURL());
  }

  @Override
  public boolean isReady() {
    return localNode.isReady() && wrapped.getSyncStatusNodePermissioningProvider()
    .map(p -> p.hasReachedSync()).orElse(true);
  }
}
