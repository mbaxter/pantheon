package tech.pegasys.pantheon.ethereum.p2p.network.permissions;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissionsBlacklist;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.util.bytes.BytesValue;

public class DefaultPeerPermissions extends PeerPermissions {

  public PeerPermissionsBlacklist getBannedNodes() {
    return bannedNodes;
  }

  public PeerPermissionsBlacklist getMisbehavingNodes() {
    return misbehavingNodes;
  }

  private final PeerPermissionsBlacklist bannedNodes;
  private final PeerPermissionsBlacklist misbehavingNodes;
  private final PeerPermissions combinedPermissions;

  private DefaultPeerPermissions(
    final PeerPermissionsBlacklist bannedNodes,
    final PeerPermissionsBlacklist misbehavingNodes,
    final List<PeerPermissions> additionalPermissions) {
    this.bannedNodes = bannedNodes;
    this.misbehavingNodes = misbehavingNodes;

    List<PeerPermissions> allPermissions = Stream.concat(Stream.of(bannedNodes, misbehavingNodes), additionalPermissions.stream())
      .collect(Collectors.toList());
    this.combinedPermissions = PeerPermissions.combine(allPermissions);
  }

  public static DefaultPeerPermissions create(final PeerPermissionsBlacklist bannedNodes,
    final PeerPermissionsBlacklist misbehavingNodes,
    final List<PeerPermissions> additionalPermissions) {
    return new DefaultPeerPermissions(bannedNodes, misbehavingNodes, additionalPermissions);
  }

  public static DefaultPeerPermissions create(final PeerPermissionsBlacklist bannedNodes,
    final PeerPermissionsBlacklist misbehavingNodes,
    final PeerPermissions... additionalPermissions) {
    return create(bannedNodes, misbehavingNodes, Arrays.asList(additionalPermissions));
  }

  @Override
  public boolean isPermitted(final Peer peer) {
    if (!combinedPermissions.isReady()) {
      // Block peers until the local node is fully up and running
      // This ensures that we can run nodePermissioningController checks
      return false;
    }
    return combinedPermissions.isPermitted(peer);
  }

  @Override
  public boolean isReady() {
    return combinedPermissions.isReady();
  }
}
