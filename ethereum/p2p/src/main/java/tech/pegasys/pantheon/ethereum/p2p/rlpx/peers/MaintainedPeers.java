package tech.pegasys.pantheon.ethereum.p2p.rlpx.peers;

import io.vertx.core.impl.ConcurrentHashSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.ConnectingToLocalNodeException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.ConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.PeerNotPermittedException;
import tech.pegasys.pantheon.util.Subscribers;

/**
 * Represents a set of peers for which connections should be actively maintained.
 */
public class MaintainedPeers {
  private final PeerPermissions peerPermissions;
  private final Set<Peer> maintainedPeers = new ConcurrentHashSet<>();
  private final Subscribers<PeerAddedCallback> addedSubscribers = new Subscribers<>();
  private final Subscribers<PeerRemovedCallback> removedCallbackSubscribers = new Subscribers<>();

  public MaintainedPeers(
    final PeerPermissions peerPermissions) {
    this.peerPermissions = peerPermissions;
  }

  public boolean add(final Peer peer) throws ConnectionException {
    if (!peerPermissions.isPermitted(peer)) {
      throw new PeerNotPermittedException();
    }

    boolean wasAdded = maintainedPeers.add(peer);
    addedSubscribers.forEach(s -> s.onPeerAdded(peer, wasAdded));
    return wasAdded;
  }

  public boolean remove(final Peer peer) {
    boolean wasRemoved = maintainedPeers.remove(peer);
    removedCallbackSubscribers.forEach(s -> s.onPeerRemoved(peer, wasRemoved));
    return wasRemoved;
  }

  public boolean contains(final Peer peer) {
    return maintainedPeers.contains(peer);
  }

  public int size() {
    return maintainedPeers.size();
  }

  public void subscribeAdd(PeerAddedCallback callback) {
    addedSubscribers.subscribe(callback);
  }

  public void subscribeRemove(PeerRemovedCallback callback) {
    removedCallbackSubscribers.subscribe(callback);
  }

  public void forEach(final Consumer<? super Peer> action) {
    maintainedPeers.forEach(action::accept);
  }

  interface PeerAddedCallback {
    void onPeerAdded(Peer peer, boolean wasAdded);
  }

  interface PeerRemovedCallback {
    void onPeerRemoved(Peer peer, boolean wasRemoved);
  }
}
