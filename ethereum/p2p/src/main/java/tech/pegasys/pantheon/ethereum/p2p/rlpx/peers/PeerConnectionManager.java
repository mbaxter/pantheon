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
package tech.pegasys.pantheon.ethereum.p2p.rlpx.peers;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Objects;
import java.util.function.Supplier;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.exceptions.P2PNetworkNotReadyException;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.RlpxAgent;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.ConnectingToLocalNodeException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.ConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.DuplicatePeerConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.PeerNotPermittedException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.TooManyPeersConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PeerConnectionManager {
  private static final Logger LOG = LogManager.getLogger();

  private final LocalNode localNode;
  private final RlpxAgent rlpxAgent;
  private final MaintainedPeers maintainedPeers;
  private final PeerPermissions peerPermissions;

  private final int maxPeers;
  private final Map<BytesValue, ManagedPeerConnection> connections = new ConcurrentHashMap<>();

  final Comparator<PeerConnection> peerPriorityComparator;

  public PeerConnectionManager(
      final LocalNode localNode,
      final RlpxAgent rlpxAgent,
      final MaintainedPeers maintainedPeers,
      final PeerPermissions peerPermissions,
      final int maxPeers) {

    this.localNode = localNode;
    this.rlpxAgent = rlpxAgent;
    this.maintainedPeers = maintainedPeers;
    this.peerPermissions = peerPermissions;
    this.maxPeers = maxPeers;

    this.peerPriorityComparator = createPeerPriorityComparator();

    rlpxAgent.subscribeConnect(this::handleConnect);
    rlpxAgent.subscribeDisconnect(this::handleDisconnect);

    maintainedPeers.subscribeRemove(this::handleMaintainedPeerRemoved);
    maintainedPeers.subscribeAdd(this::handleMaintainedPeerAdded);

    peerPermissions.subscribeUpdate(this::disconnectDisallowed);
  }

  /**
   * Runs some validations on the given peer before attempting to initiate a connection.
   * @param peer The peer to connect to.
   * @return A future that resolves to a {@link PeerConnection} on success.
   */
  public CompletableFuture<PeerConnection> maybeConnect(final Peer peer) {
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
    if (!localNode.isReady()) {
      connectionFuture.completeExceptionally(new P2PNetworkNotReadyException());
      return connectionFuture;
    }
    Peer localPeer = localNode.getPeer().get();
    if (Objects.equals(localPeer.getId(), peer.getId())) {
      // Don't connect to our self
      connectionFuture.completeExceptionally(new ConnectingToLocalNodeException());
      return connectionFuture;
    }

    final OutgoingConnection outgoingConnection = new OutgoingConnection(() -> rlpxAgent.connect(peer));
    final ManagedPeerConnection existingConnection =
      connections.putIfAbsent(peer.getId(), outgoingConnection);
    if (existingConnection != null) {
      LOG.debug("Attempted to connect to already connected peer: {}", peer.getId());
      ConnectionException exception =
        new DuplicatePeerConnectionException("Attempt to connect to already connecting peer");
      connectionFuture.completeExceptionally(exception);
      return connectionFuture;
    }

    if (connections.size() > maxPeers) {
      connectionFuture.completeExceptionally(new TooManyPeersConnectionException());
    } else if (!isPeerAllowed(peer)) {
      connectionFuture.completeExceptionally(new PeerNotPermittedException());
    }

    if (!connectionFuture.isCompletedExceptionally()) {
      // Try to connect if we haven't failed the future already
      outgoingConnection.initiate()
        .whenComplete(
          (peerConnection, err) -> {
            if (err != null) {
              connectionFuture.completeExceptionally(err);
              return;
            }
            connectionFuture.complete(peerConnection);
          });
    }

    return connectionFuture.whenComplete(
      (peerConnection, err) -> {
        if (err != null) {
          LOG.debug("Unable to connect to peer {}. {}", peer.getId(), err.toString());
          connections.computeIfPresent(peer.getId(), (id, managedConn) -> managedConn.connectionFailedToEstablish() ? null : managedConn);
        }
      });
  }

  public boolean disconnect(final Peer peer) {
    final ManagedPeerConnection managedPeer = connections.get(peer.getId());
    if (managedPeer == null) {
      return false;
    }

    managedPeer.manage((conn) -> conn.disconnect(DisconnectReason.REQUESTED));
    return true;
  }

  public void checkMaintainedPeers() {
    maintainedPeers.forEach(peer -> {
      BytesValue peerId = peer.getId();
      if (!connections.containsKey(peerId)) {
        maybeConnect(peer);
      }
    });
  }

  public void connectToAvailablePeers(Supplier<Stream<? extends Peer>> availablePeers) {
    final int availablePeerSlots = Math.max(0, maxPeers - connections.size());
    if (availablePeerSlots <= 0) {
      return;
    }

    final List<Peer> peers =
      availablePeers
        .get()
        .filter(peer -> !connections.containsKey(peer.getId()))
        .filter(this::isPeerAllowed)
        .collect(Collectors.toList());
    Collections.shuffle(peers);
    if (peers.size() == 0) {
      return;
    }

    LOG.trace(
      "Initiating connection to {} peers from the peer table",
      Math.min(availablePeerSlots, peers.size()));
    peers.stream().limit(availablePeerSlots).forEach(this::maybeConnect);
  }

  private void handleConnect(final PeerConnection connection, final boolean initatedByPeer) {
    BytesValue peerId = connection.getPeer().getId();
    final ManagedPeerConnection establishedConnection = new EstablishedConnection(connection, initatedByPeer);
    final ManagedPeerConnection existingConnection =
      connections.put(peerId, establishedConnection);

    if (existingConnection != null) {
      existingConnection.manage(conn -> {
        if (conn == connection) {
          // Connections are the same - nothing to do
          // This can happen when we initiate a connection in maybeConnect()
          // and then the connection is established and flows back through this handler
        }
        if (!existingConnection.isDisconnected()) {
          // Nothing to do, the original connection was terminated
        }
        // Keep the existing connection and terminate the newer connection
        connections.put(peerId, existingConnection);
        connection.disconnect(DisconnectReason.ALREADY_CONNECTED);
      });
    }

    if (!isPeerAllowed(connection)) {
      LOG.debug("Disconnect from disallowed peer: {}", peerId);
      connection.disconnect(DisconnectReason.UNKNOWN);
      connections.remove(peerId);
    }

    enforceLimits();
  }

  private boolean isPeerAllowed(PeerConnection connection) {
    return isPeerAllowed(connection.getPeer());
  }

  private boolean isPeerAllowed(Peer peer) {
    return peerPermissions.isPermitted(peer);
  }

  private void handleMaintainedPeerAdded(final Peer peer, final boolean wasAdded) {
    maybeConnect(peer);
  }

  private void handleMaintainedPeerRemoved(final Peer peer, final boolean wasRemoved) {
    ManagedPeerConnection managedPeerConnection = connections.get(peer.getId());
    if (managedPeerConnection == null) {
      return;
    }
    managedPeerConnection.manage(conn -> conn.disconnect(DisconnectReason.REQUESTED));
  }

  private void disconnectDisallowed() {
    connections.values().forEach(this::disconnectIfDisallowed);
  }

  private void disconnectIfDisallowed(final ManagedPeerConnection managedPeerConnection) {
    managedPeerConnection.manage(
        (peerConnection) -> {
          if (!isPeerAllowed(peerConnection.getPeer())) {
            peerConnection.disconnect(DisconnectReason.REQUESTED);
          }
        });
  }

  private void enforceLimits() {
    AtomicBoolean limitsExceeded = new AtomicBoolean(false);
    rlpxAgent.getConnections()
        .sorted(peerPriorityComparator)
        .skip(maxPeers)
        .filter(
            (p) -> {
              boolean isMaintained = isMaintained(p);
              if (isMaintained) {
                limitsExceeded.set(true);
              }
              return !isMaintained;
            })
        .forEach(p -> p.disconnect(DisconnectReason.TOO_MANY_PEERS));

    if (limitsExceeded.get()) {
      LOG.warn(
          "Max peer limit ({}) exceeded due to too many maintained peers ({}).",
          maxPeers,
          maintainedPeers.size());
    }
  }

  private boolean isMaintained(final PeerConnection peerConnection) {
    return maintainedPeers.contains(peerConnection.getPeer());
  }

  private void handleDisconnect(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    BytesValue peerId = connection.getPeer().getId();
    connections.computeIfPresent(peerId, (id, peer) -> peer.equals(connection) ? null : peer);
  }

  private Comparator<PeerConnection> createPeerPriorityComparator() {
    final Comparator<PeerConnection> maintainedPeerComparator =
      Comparator.comparing((PeerConnection p) -> maintainedPeers.contains(p.getPeer()) ? 0 : 1);
    final Comparator<PeerConnection> longestConnectionComparator =
      Comparator.comparing(PeerConnection::getConnectedAt);

    // Prefer maintained peers, and then peers who have been connected longer
    return maintainedPeerComparator.thenComparing(longestConnectionComparator);
  }

  private interface ManagedPeerConnection {
    void manage(Consumer<PeerConnection> connectionHandler);

    boolean wasInitiatedRemotely();

    boolean connectionSuccessfullyEstablished();

    boolean connectionFailedToEstablish();

    boolean isDisconnected();

    Optional<PeerConnection> getConnection();
  }

  private static class EstablishedConnection implements ManagedPeerConnection {
    private final PeerConnection connection;
    private final boolean wasInitiatedRemotely;

    public EstablishedConnection(final PeerConnection connection, final boolean wasInitiatedRemotely) {
      this.connection = connection;
      this.wasInitiatedRemotely = wasInitiatedRemotely;
    }

    @Override
    public void manage(final Consumer<PeerConnection> connectionHandler) {
      connectionHandler.accept(connection);
    }

    @Override
    public boolean wasInitiatedRemotely() {
      return wasInitiatedRemotely;
    }

    @Override
    public boolean connectionSuccessfullyEstablished() {
      return true;
    }

    @Override
    public boolean connectionFailedToEstablish() {
      return false;
    }

    @Override
    public boolean isDisconnected() {
      return connection.isDisconnected();
    }

    @Override
    public Optional<PeerConnection> getConnection() {
      return Optional.of(connection);
    }

    public PeerConnection getConnectionNow() {
      return connection;
    }
  }

  private static class OutgoingConnection implements ManagedPeerConnection {

    private final Supplier<CompletableFuture<PeerConnection>> connector;
    private CompletableFuture<PeerConnection> pendingConnection = null;

    private final AtomicBoolean connectionInitiated = new AtomicBoolean(false);

    private OutgoingConnection(final Supplier<CompletableFuture<PeerConnection>> connector) {
      this.connector = connector;
    }

    public CompletableFuture<PeerConnection> initiate() {
      if (connectionInitiated.compareAndSet(false, true)) {
        pendingConnection = connector.get();
      }
      return this.pendingConnection;
    }

    @Override
    public void manage(final Consumer<PeerConnection> connectionHandler) {
      pendingConnection.thenAccept(connectionHandler);
    }

    @Override
    public boolean wasInitiatedRemotely() {
      return false;
    }

    @Override
    public boolean connectionSuccessfullyEstablished() {
      return connectionInitiated.get() && pendingConnection.isDone() && !pendingConnection.isCompletedExceptionally() && !pendingConnection.isCancelled();
    }

    @Override
    public boolean connectionFailedToEstablish() {
      return connectionInitiated.get() && pendingConnection.isDone() && (pendingConnection.isCompletedExceptionally() || pendingConnection.isCancelled());
    }

    @Override
    public boolean isDisconnected() {
      return getConnection().map(PeerConnection::isDisconnected).orElse(false);
    }

    @Override
    public Optional<PeerConnection> getConnection() {
      try {
        return Optional.ofNullable(pendingConnection.getNow(null));
      } catch (Exception e) {
        return Optional.empty();
      }
    }
  }
}
