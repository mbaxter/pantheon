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
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricCategory;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.impl.ConcurrentHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class PeerConnectionManager {
  private static final Logger LOG = LogManager.getLogger();

  private final Map<BytesValue, ManagedPeerConnection> connections = new ConcurrentHashMap<>();

  private final PeerConnector peerConnector;
  private final DiscoveryPeerSupplier discoveryPeers;
  private final int maxPeers;
  private final Set<Peer> maintainedPeers = new ConcurrentHashSet<>();
  private final PeerBlacklist peerBlacklist;

  private Optional<NodePermissioningController> nodePermissioningController = Optional.empty();
  private EnodeURL ourEnodeUrl;

  private boolean enabled = false;
  private List<ShutdownCallback> shutdownCallbacks = new ArrayList<>();

  private final LabelledMetric<Counter> disconnectCounter;
  private final Counter connectedPeersCounter;

  @VisibleForTesting final Comparator<PeerConnection> peerPriorityComparator;

  public PeerConnectionManager(
      final PeerConnector peerConnector,
      final DiscoveryPeerSupplier discoveryPeers,
      final int maxPeers,
      final PeerBlacklist peerBlacklist,
      final MetricsSystem metrics) {
    disconnectCounter =
        metrics.createLabelledCounter(
            MetricCategory.PEERS,
            "disconnected_total",
            "Total number of peers disconnected",
            "initiator",
            "disconnectReason");
    connectedPeersCounter =
        metrics.createCounter(
            MetricCategory.PEERS, "connected_total", "Total number of peers connected");
    metrics.createGauge(
        MetricCategory.PEERS,
        "peer_count_current",
        "Number of peers currently connected",
        () -> (double) countActiveConnections());
    this.peerConnector = peerConnector;
    this.discoveryPeers = discoveryPeers;
    this.maxPeers = maxPeers;
    this.peerBlacklist = peerBlacklist;
    this.shutdownCallbacks.add(this::shutdownPeerConnections);
    this.peerPriorityComparator = createPeerPriorityComparator();
  }

  private Comparator<PeerConnection> createPeerPriorityComparator() {
    final Comparator<PeerConnection> maintainedPeerComparator =
        Comparator.comparing((PeerConnection p) -> maintainedPeers.contains(p.getPeer()) ? 0 : 1);
    final Comparator<PeerConnection> longestConnectionComparator =
        Comparator.comparing(PeerConnection::getConnectedAt);

    // Prefer maintained peers, and then peers who have been connected longer
    return maintainedPeerComparator.thenComparing(longestConnectionComparator);
  }

  public void setNodePermissioningController(
      final NodePermissioningController nodePermissioningController, final Blockchain blockchain) {
    checkNotNull(nodePermissioningController);
    checkNotNull(blockchain);
    final long blockAddedObserverId =
        blockchain.observeBlockAdded((e, b) -> disconnectDisallowed());
    shutdownCallbacks.add(() -> blockchain.removeObserver(blockAddedObserverId));
  }

  public void setEnodeUrl(final EnodeURL ourEnodeUrl) {
    this.ourEnodeUrl = ourEnodeUrl;
  }

  public void setEnabled() {
    checkState(
        ourEnodeUrl != null,
        "Local enode must be set before " + getClass().getSimpleName() + " can be enabled.");
    enabled = true;
  }

  private void disconnectDisallowed() {
    connections.values().forEach(this::disconnectIfDisallowed);
  }

  private void disconnectIfDisallowed(final ManagedPeerConnection managedPeerConnection) {
    managedPeerConnection.manage(
        (peerConnection) -> {
          if (!isPeerAllowed(peerConnection)) {
            peerConnection.disconnect(DisconnectReason.REQUESTED);
          }
        });
  }

  public int countActiveConnections() {
    return Math.toIntExact(getConnections().count());
  }

  public Stream<PeerConnection> getConnections() {
    return connections.values().stream()
        .map(ManagedPeerConnection::getConnection)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  public boolean handleIncomingConnection(final PeerConnection connection) {
    registerConnection();
    boolean connectionAccepted = true;
    BytesValue peerId = connection.getPeer().getId();
    final ManagedPeerConnection incomingConnection = new IncomingConnection(connection);
    final ManagedPeerConnection existingConnection =
        connections.putIfAbsent(peerId, incomingConnection);
    if (existingConnection != null) {
      if (!existingConnection.isDisconnected()) {
        // We're already talking to this peer
        connection.disconnect(DisconnectReason.ALREADY_CONNECTED);
      } else {
        // Replace the existing connection
        connections.put(peerId, incomingConnection);
      }
    }

    if (!isPeerAllowed(connection)) {
      LOG.debug("Disconnect incoming connection from disallowed peer: {}", peerId);
      connection.disconnect(DisconnectReason.UNKNOWN);
      connections.remove(peerId);
      connectionAccepted = false;
    }

    if (connectionAccepted) {
      LOG.debug("Successfully accepted connection from {}", peerId);
    }

    enforceLimits();
    return connectionAccepted;
  }

  public CompletableFuture<PeerConnection> maybeConnect(final Peer peer) {
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
    final ManagedPeerConnection outgoingConnection = new OutgoingConnection(connectionFuture);
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
            registerConnection();
            // Make sure we are still within maxPeers limit
            enforceLimits();
          }
        });
  }

  private void enforceLimits() {
    AtomicBoolean limitsExceeded = new AtomicBoolean(false);
    getConnections()
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

  public void checkMaintainedPeers() {
    for (final Peer peer : maintainedPeers) {
      BytesValue peerId = peer.getId();
      if (!connections.containsKey(peerId)) {
        maybeConnect(peer);
      }
    }
  }

  @VisibleForTesting
  void connectToAvailablePeers() {
    final int availablePeerSlots = Math.max(0, maxPeers - connections.size());
    if (availablePeerSlots <= 0) {
      return;
    }

    final List<DiscoveryPeer> peers =
        discoveryPeers
            .get()
            .filter(peer -> peer.getStatus() == PeerDiscoveryStatus.BONDED)
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
    connections
        .get(peer.getId())
        .manage(
            (peerConnection) -> {
              peerConnection.disconnect(DisconnectReason.REQUESTED);
            });

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
    return isPeerAllowed(peerConnection.getPeer().getId(), peerConnection.getRemoteEnodeURL());
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

  public void registerConnection() {
    connectedPeersCounter.inc();
  }

  public void handleDisconnect(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    BytesValue peerId = connection.getPeer().getId();
    connections.computeIfPresent(peerId, (id, peer) -> peer.equals(connection) ? null : peer);
    disconnectCounter.labels(initiatedByPeer ? "remote" : "local", reason.name()).inc();
  }

  public void shutdown() {
    shutdownCallbacks.forEach(ShutdownCallback::onShutdown);
  }

  private void shutdownPeerConnections() {
    getConnections().forEach((conn) -> conn.disconnect(DisconnectReason.CLIENT_QUITTING));
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

  private interface ManagedPeerConnection {
    void manage(Consumer<PeerConnection> connectionHandler);

    boolean wasInitiatedRemotely();

    boolean isPending();

    boolean isDisconnected();

    Optional<PeerConnection> getConnection();
  }

  private static class IncomingConnection implements ManagedPeerConnection {
    private final PeerConnection connection;

    public IncomingConnection(final PeerConnection connection) {
      this.connection = connection;
    }

    @Override
    public void manage(final Consumer<PeerConnection> connectionHandler) {
      connectionHandler.accept(connection);
    }

    @Override
    public boolean wasInitiatedRemotely() {
      return true;
    }

    @Override
    public boolean isPending() {
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
  }

  private static class OutgoingConnection implements ManagedPeerConnection {
    private final CompletableFuture<PeerConnection> pendingConnection;
    private final AtomicBoolean isPending = new AtomicBoolean(true);

    private OutgoingConnection(final CompletableFuture<PeerConnection> pendingConnection) {
      this.pendingConnection = pendingConnection;
      this.pendingConnection.whenComplete(
          (res, err) -> {
            isPending.set(false);
          });
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
    public boolean isPending() {
      return isPending.get();
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
