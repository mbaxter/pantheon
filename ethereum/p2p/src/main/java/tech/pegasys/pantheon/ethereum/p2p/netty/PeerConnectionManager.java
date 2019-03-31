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
package tech.pegasys.pantheon.ethereum.p2p.netty;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryStatus;
import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.ConnectingToLocalNodeException;
import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.ConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.DuplicatePeerConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.PeerNotPermittedException;
import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.TooManyPeersConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.impl.ConcurrentHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class PeerConnectionManager {
  private static final Logger LOG = LogManager.getLogger();

  private final PeerConnectionRegistry activeConnections;
  private final Map<BytesValue, CompletableFuture<PeerConnection>> pendingConnections =
      new ConcurrentHashMap<>();

  private final PeerConnector peerConnector;
  private final DiscoveryPeerSupplier discoveryPeers;
  private final int maxPeers;
  private final Set<Peer> maintainedPeers = new ConcurrentHashSet<>();
  private final PeerBlacklist peerBlacklist;

  private Optional<NodePermissioningController> nodePermissioningController = Optional.empty();
  private EnodeURL ourEnodeUrl;

  private boolean enabled = false;
  private List<ShutdownCallback> shutdownCallbacks = new ArrayList<>();

  public PeerConnectionManager(
      final PeerConnector peerConnector,
      final DiscoveryPeerSupplier discoveryPeers,
      final int maxPeers,
      final PeerBlacklist peerBlacklist,
      final MetricsSystem metrics) {
    this.peerConnector = peerConnector;
    this.discoveryPeers = discoveryPeers;
    this.maxPeers = maxPeers;
    this.activeConnections = new PeerConnectionRegistry(metrics);
    this.peerBlacklist = peerBlacklist;
    this.shutdownCallbacks.add(this::shutdownPeerConnections);
  }

  public void setNodePermissioningController(
      final NodePermissioningController nodePermissioningController, final Blockchain blockchain) {
    checkNotNull(nodePermissioningController);
    checkNotNull(blockchain);
    final long blockAddedObserverId =
        blockchain.observeBlockAdded((e, b) -> disconnectInvalidPeers());
    shutdownCallbacks.add(() -> blockchain.removeObserver(blockAddedObserverId));
  }

  public void setEnodeUrl(final EnodeURL ourEnodeURL) {
    this.ourEnodeUrl = ourEnodeURL;
  }

  public void setEnabled() {
    checkState(
        ourEnodeUrl != null,
        "Local enode must be set before " + getClass().getSimpleName() + " can be enabled.");
    enabled = true;
  }

  public void disconnectInvalidPeers() {
    activeConnections.getPeerConnections().stream()
        .filter(p -> !isPeerAllowed(p))
        .forEach(p -> p.disconnect(DisconnectReason.REQUESTED));
  }

  public int countActiveConnections() {
    return activeConnections.size();
  }

  public int countPendingConnections() {
    return pendingConnections.size();
  }

  public int countAllConnections() {
    return countActiveConnections() + countPendingConnections();
  }

  public PeerConnectionRegistry getActiveConnections() {
    return activeConnections;
  }

  @VisibleForTesting
  boolean isConnecting(final BytesValue peerId) {
    return pendingConnections.containsKey(peerId);
  }

  @VisibleForTesting
  boolean isConnected(final BytesValue peerId) {
    return activeConnections.isAlreadyConnected(peerId);
  }

  public boolean handleIncomingConnection(final PeerConnection connection) {
    boolean connectionAccepted = true;
    BytesValue peerId = connection.getPeer().getNodeId();
    final CompletableFuture<PeerConnection> connectionFuture =
        CompletableFuture.completedFuture(connection);
    final CompletableFuture<PeerConnection> existingPendingConnection =
        pendingConnections.put(peerId, connectionFuture);
    if (existingPendingConnection != null) {
      // Disconnect any existing pending connection since we know this connection is already live
      existingPendingConnection.whenComplete(
          (peerConnection, err) -> {
            if (err != null) {
              return;
            }
            peerConnection.disconnect(DisconnectReason.ALREADY_CONNECTED);
          });
    }

    if (!isPeerAllowed(connection)) {
      LOG.debug("Disconnect incoming connection from disallowed peer: {}", peerId);
      connection.disconnect(DisconnectReason.UNKNOWN);
      connectionAccepted = false;
    }

    if (connectionAccepted) {
      activeConnections.registerConnection(connection);
      LOG.debug("Successfully accepted connection from {}", connection.getPeer().getNodeId());
      logConnections();
    }
    pendingConnections.remove(peerId);

    // Reject incoming connections if we've reached our limit
    if (countActiveConnections() > maxPeers) {
      LOG.debug(
          "Disconnecting incoming connection because connection limit of {} has been reached: {}",
          maxPeers,
          connection.getPeer().getNodeId());
      connection.disconnect(DisconnectReason.TOO_MANY_PEERS);
    }

    // Make sure we are still within maxPeers limit
    enforceMaxPeers();
    return connectionAccepted;
  }

  public CompletableFuture<PeerConnection> maybeConnect(final Peer peer) {
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();

    final CompletableFuture<PeerConnection> existingPendingConnection =
        pendingConnections.putIfAbsent(peer.getId(), connectionFuture);
    if (existingPendingConnection != null) {
      LOG.debug("Attempted to connect to peer with pending connection: {}", peer.getId());
      ConnectionException exception =
          new DuplicatePeerConnectionException("Attempt to connect to already connecting peer");
      connectionFuture.completeExceptionally(exception);
      return connectionFuture;
    }

    if (countAllConnections() >= maxPeers) {
      connectionFuture.completeExceptionally(new TooManyPeersConnectionException());
    } else if (!isPeerAllowed(peer)) {
      connectionFuture.completeExceptionally(new PeerNotPermittedException());
    } else if (isConnected(peer.getId())) {
      connectionFuture.completeExceptionally(
          new DuplicatePeerConnectionException("Attempt to connect to already connected peer"));
    }

    if (!connectionFuture.isCompletedExceptionally()) {
      // Try to connect if we haven't failed the future already
      peerConnector
          .connect(peer)
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
          } else {
            activeConnections.registerConnection(peerConnection);
            logConnections();
          }

          pendingConnections.remove(peer.getId());

          // Make sure we are still within maxPeers limit
          enforceMaxPeers();
        });
  }

  private void enforceMaxPeers() {
    if (countActiveConnections() > maxPeers) {
      final AtomicInteger disconnected = new AtomicInteger(0);
      activeConnections.getPeerConnections().stream()
          .sorted(Comparator.comparingLong(PeerConnection::getConnectedAt))
          .skip(maxPeers)
          .forEach(
              (peer) -> {
                disconnected.incrementAndGet();
                peer.disconnect(DisconnectReason.TOO_MANY_PEERS);
              });

      if (disconnected.get() > 0) {
        LOG.debug(
            "Disconnected {} peers because connection limit of {} exceeded",
            disconnected.get(),
            maxPeers);
      }
    }
  }

  public void checkMaintainedPeers() {
    for (final Peer peer : maintainedPeers) {
      BytesValue peerId = peer.getId();
      if (!(isConnecting(peerId) || isConnected(peerId))) {
        maybeConnect(peer);
      }
    }
  }

  @VisibleForTesting
  void connectToAvailablePeers() {
    final int availablePeerSlots = Math.max(0, maxPeers - countAllConnections());
    if (availablePeerSlots <= 0) {
      return;
    }

    final List<DiscoveryPeer> peers =
        discoveryPeers
            .get()
            .filter(peer -> peer.getStatus() == PeerDiscoveryStatus.BONDED)
            .filter(peer -> !isConnected(peer.getId()) && !isConnecting(peer.getId()))
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

  public boolean addMaintainedPeer(final Peer peer) {
    assertEnabled();
    assertPeerValid(peer);
    if (!isPeerAllowed(peer)) {
      throw new PeerNotPermittedException();
    }

    final boolean added = maintainedPeers.add(peer);
    if (added) {
      maybeConnect(peer);
      return true;
    } else {
      return false;
    }
  }

  public boolean removeMaintainedPeer(final Peer peer) {
    final boolean removed = maintainedPeers.remove(peer);

    final CompletableFuture<PeerConnection> connectionFuture = pendingConnections.get(peer.getId());
    if (connectionFuture != null) {
      connectionFuture.thenAccept(connection -> connection.disconnect(DisconnectReason.REQUESTED));
    }

    final Optional<PeerConnection> peerConnection =
        activeConnections.getConnectionForPeer(peer.getId());
    peerConnection.ifPresent(pc -> pc.disconnect(DisconnectReason.REQUESTED));

    return removed;
  }

  @VisibleForTesting
  Set<Peer> getMaintainedPeers() {
    return Collections.unmodifiableSet(maintainedPeers);
  }

  private void assertPeerValid(final Peer peer) {
    if (peer.getId().equals(ourEnodeUrl.getNodeId())) {
      throw new ConnectingToLocalNodeException();
    }
  }

  private boolean isPeerAllowed(final PeerConnection peerConnection) {
    return isPeerAllowed(peerConnection.getPeer().getNodeId(), peerConnection.getRemoteEnodeURL());
  }

  private boolean isPeerAllowed(final Peer peer) {
    return isPeerAllowed(peer.getId(), peer.getEnodeURL());
  }

  private boolean isPeerAllowed(final BytesValue peerId, final EnodeURL peerEnode) {
    assertEnabled();
    if (peerBlacklist.contains(peerId)) {
      return false;
    }

    return nodePermissioningController.map(c -> c.isPermitted(ourEnodeUrl, peerEnode)).orElse(true);
  }

  public void handleDisconnect(
      final PeerConnection connection, final DisconnectReason reason, final boolean initiatedByPeer) {
    activeConnections.handleDisconnect(connection, reason, initiatedByPeer);
  }

  private void logConnections() {
    LOG.debug(
        "Connections: {} pending, {} active connections.",
        pendingConnections.size(),
        activeConnections.size());
  }

  public void shutdown() {
    shutdownCallbacks.forEach(ShutdownCallback::onShutdown);
  }

  private void shutdownPeerConnections() {
    activeConnections
        .getPeerConnections()
        .forEach(p -> p.disconnect(DisconnectReason.CLIENT_QUITTING));
  }

  private void assertEnabled() {
    checkState(enabled, "Invalid call to disabled " + getClass().getSimpleName());
  }

  @FunctionalInterface
  private interface ShutdownCallback {
    void onShutdown();
  }

  @FunctionalInterface
  public interface PeerConnector {
    CompletableFuture<PeerConnection> connect(final Peer peer);
  }

  @FunctionalInterface
  public interface DiscoveryPeerSupplier {
    Stream<DiscoveryPeer> get();
  }
}
