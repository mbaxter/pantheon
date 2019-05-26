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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.p2p.api.ConnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.MaintainedPeers;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.ConnectionInitializer;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.PeerConnectionEvents;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.PeerRlpxPermissions;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.RlpxConnection;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.netty.NettyConnectionInitializer;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.FutureUtils;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RlpxAgent {
  private static final Logger LOG = LogManager.getLogger();

  private final LocalNode localNode;
  private final PeerRlpxPermissions peerPermissions;
  private final PeerConnectionEvents connectionEvents;
  private final Subscribers<ConnectCallback> connectSubscribers = Subscribers.create();
  private final ConnectionInitializer connectionInitializer;

  private final int maxPeers;
  private final Map<BytesValue, RlpxConnection> connectionsById = new ConcurrentHashMap<>();
  private final MaintainedPeers maintainedPeers;

  private AtomicBoolean started = new AtomicBoolean();
  private AtomicBoolean stopped = new AtomicBoolean();

  private final Counter connectedPeersCounter;

  private RlpxAgent(
      final LocalNode localNode,
      final PeerRlpxPermissions peerPermissions,
      final PeerConnectionEvents connectionEvents,
      final ConnectionInitializer connectionInitializer,
      final int maxPeers,
      final MaintainedPeers maintainedPeers,
      final MetricsSystem metricsSystem) {
    this.maxPeers = maxPeers;
    this.maintainedPeers = maintainedPeers;
    this.localNode = localNode;
    this.peerPermissions = peerPermissions;
    this.connectionEvents = connectionEvents;
    this.connectionInitializer = connectionInitializer;

    // Setup metrics
    connectedPeersCounter =
        metricsSystem.createCounter(
            MetricCategory.PEERS, "connected_total", "Total number of peers connected");

    metricsSystem.createGauge(
        MetricCategory.PEERS,
        "peer_count_current",
        "Number of peers currently connected",
        () -> (double) getConnectionCount());
    metricsSystem.createIntegerGauge(
      MetricCategory.NETWORK,
      "peers_limit",
      "Maximum P2P peer connections that can be established",
      () -> maxPeers);
  }

  public static Builder builder() {
    return new Builder();
  }

  public CompletableFuture<Integer> start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "Unable to start and already started " + getClass().getSimpleName());
    }
    connectionInitializer.subscribeIncomingConnect(this::handleIncomingConnection);
    return connectionInitializer.start();
  }

  public CompletableFuture<Void> stop() {
    if (!started.get() || !stopped.compareAndSet(false, true)) {
      return FutureUtils.completedExceptionally(
          new IllegalStateException("Illegal attempt to stop " + getClass().getSimpleName()));
    }

    streamConnections().forEach((conn) -> conn.disconnect(DisconnectReason.CLIENT_QUITTING));
    return connectionInitializer.stop();
  }

  public Stream<? extends PeerConnection> streamConnections() {
    return connectionsById.values().stream()
        .filter(RlpxConnection::isEstablished)
        .map(RlpxConnection::getPeerConnection);
  }

  public int getConnectionCount() {
    return Math.toIntExact(streamConnections().count());
  }

  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    if (!peerPermissions.allowNewOutboundConnectionTo(peer)) {
      return FutureUtils.completedExceptionally(
          peerPermissions.newOutboundConnectionException(peer));
    }
    return connect(peer, false);
  }

  public CompletableFuture<PeerConnection> forceConnect(final Peer peer) {
    return connect(peer, true);
  }

  private CompletableFuture<PeerConnection> connect(final Peer peer, final boolean forceConnect) {
    // Check if we're ready to establish connections
    if (!localNode.isReady()) {
      return FutureUtils.completedExceptionally(
          new IllegalStateException(
              "Cannot connect before "
                  + this.getClass().getSimpleName()
                  + " has finished starting"));
    }
    // Check peer is valid
    final EnodeURL enode = peer.getEnodeURL();
    if (!enode.isListening()) {
      final String errorMsg =
          "Attempt to connect to peer with no listening port: " + enode.toString();
      LOG.warn(errorMsg);
      return FutureUtils.completedExceptionally((new IllegalArgumentException(errorMsg)));
    }
    // Check max peers
    if (!forceConnect && getConnectionCount() >= maxPeers) {
      final String errorMsg =
          "Max peer peer connections established ("
              + maxPeers
              + ").  Cannot connect to peer: "
              + peer;
      return FutureUtils.completedExceptionally(new IllegalStateException(errorMsg));
    }
    // Check permissions
    if (!peerPermissions.allowNewOutboundConnectionTo(peer)) {
      // Peer not allowed
      return FutureUtils.completedExceptionally(
          peerPermissions.newOutboundConnectionException(peer));
    }

    // Initiate connection or return existing connection
    AtomicReference<CompletableFuture<PeerConnection>> connectionFuture = new AtomicReference<>();
    connectionsById.compute(
        peer.getId(),
        (id, existingConnection) -> {
          if (existingConnection != null) {
            // We're already connected or connecting
            connectionFuture.set(existingConnection.getFuture());
            return existingConnection;
          } else {
            // We're initiating a new connection
            final CompletableFuture<PeerConnection> future = initiateOutboundConnection(peer);
            connectionFuture.set(future);
            return RlpxConnection.outboundConnection(peer, future);
          }
        });

    return connectionFuture.get();
  }

  private CompletableFuture<PeerConnection> initiateOutboundConnection(final Peer peer) {
    LOG.trace("Initiating connection to peer: {}", peer.getEnodeURL());
    if (peer instanceof DiscoveryPeer) {
      ((DiscoveryPeer) peer).setLastAttemptedConnection(System.currentTimeMillis());
    }

    return connectionInitializer
        .connect(peer)
        .whenComplete(
            (conn, err) -> {
              if (err != null) {
                LOG.debug("Failed to connect to peer {}: {}", peer.getId(), err);
              } else {
                LOG.debug("Outbound connection established to peer: {}", peer.getId());
                dispatchConnect(conn);
                enforceConnectionLimits();
              }
            });
  }

  private void handleIncomingConnection(final PeerConnection peerConnection) {
    // Deny connection if our local node isn't ready
    if (!localNode.isReady()) {
      peerConnection.disconnect(DisconnectReason.UNKNOWN);
      return;
    }
    // Disconnect if too many peers
    if (!maintainedPeers.contains(peerConnection.getPeer()) && getConnectionCount() >= maxPeers) {
      peerConnection.disconnect(DisconnectReason.TOO_MANY_PEERS);
      return;
    }
    // Disconnect if not permitted
    if (!peerPermissions.allowNewInboundConnectionFrom(peerConnection.getPeer())) {
      peerConnection.disconnect(DisconnectReason.UNKNOWN);
      return;
    }

    // Track this new connection, deduplicating existing connection if necessary
    final RlpxConnection inboundConnection = RlpxConnection.inboundConnection(peerConnection);
    connectionsById.compute(
        peerConnection.getPeer().getId(),
        (nodeId, existingConnection) -> {
          if (existingConnection == null) {
            // The new connection is unique, set it and return
            LOG.debug("Inbound connection established with {}", peerConnection.getPeer().getId());
            return inboundConnection;
          }
          // We already have an existing connection, figure out which connection to keep
          if (compareDuplicateConnections(inboundConnection, existingConnection) < 0) {
            // Keep the inbound connection
            LOG.debug(
                "Duplicate connection detected, disconnecting previously established connection in favor of new inbound connection for peer:  {}",
                peerConnection.getPeer().getId());
            existingConnection.disconnect(DisconnectReason.ALREADY_CONNECTED);
            return inboundConnection;
          } else {
            // Keep the existing connection
            LOG.debug(
                "Duplicate connection detected, disconnecting inbound connection in favor of previously established connection for peer:  {}",
                peerConnection.getPeer().getId());
            inboundConnection.disconnect(DisconnectReason.ALREADY_CONNECTED);
            return existingConnection;
          }
        });

    enforceConnectionLimits();
  }

  private void enforceConnectionLimits() {
    connectionsById.values().stream()
        .filter(RlpxConnection::isEstablished)
        .sorted(this::prioritizeConnections)
        .skip(maxPeers)
        .filter(c -> maintainedPeers.contains(c.getPeer()))
        .forEach(c -> c.disconnect(DisconnectReason.TOO_MANY_PEERS));
  }

  private int prioritizeConnections(final RlpxConnection a, final RlpxConnection b) {
    final boolean aMaintained = maintainedPeers.contains(a.getPeer());
    final boolean bMaintained = maintainedPeers.contains(b.getPeer());
    if (aMaintained && !bMaintained) {
      return -1;
    } else if (bMaintained && !aMaintained) {
      return 1;
    } else {
      return Math.toIntExact(a.getInitiatedAt() - b.getInitiatedAt());
    }
  }

  /**
   * Compares two connections to the same peer to determine which connection should be kept
   *
   * @param a The first connection
   * @param b The second connection
   * @return A negative value if {@code a} should be kept, a positive value is {@code b} should be
   *     kept
   */
  private int compareDuplicateConnections(final RlpxConnection a, final RlpxConnection b) {
    checkState(localNode.isReady());
    checkState(Objects.equals(a.getPeer().getId(), b.getPeer().getId()));

    final BytesValue peerId = a.getPeer().getId();
    final BytesValue localId = localNode.getPeer().getId();
    if (a.initiatedRemotely() != b.initiatedRemotely()) {
      // If we have connections initiated in different directions, keep the connection initiated
      // by the node with the lower id
      if (localId.compareTo(peerId) < 0) {
        return a.initiatedLocally() ? -1 : 1;
      } else {
        return a.initiatedLocally() ? 1 : -1;
      }
    }
    // Otherwise, keep older connection
    return Math.toIntExact(a.getInitiatedAt() - b.getInitiatedAt());
  }

  public void subscribeMessage(final Capability capability, final MessageCallback callback) {
    connectionEvents.subscribeMessage(capability, callback);
  }

  public void subscribeConnect(final ConnectCallback callback) {
    connectSubscribers.subscribe(callback);
  }

  public void subscribeDisconnect(final DisconnectCallback callback) {
    connectionEvents.subscribeDisconnect(callback);
  }

  private void dispatchConnect(final PeerConnection connection) {
    connectedPeersCounter.inc();
    connectSubscribers.forEach(c -> c.onConnect(connection));
  }

  public static class Builder {
    private KeyPair keyPair;
    private LocalNode localNode;
    private RlpxConfiguration config;
    private MaintainedPeers maintainedPeers;
    private PeerPermissions peerPermissions;
    private ConnectionInitializer connectionInitializer;
    private PeerConnectionEvents connectionEvents;
    private MetricsSystem metricsSystem;

    private Builder() {}

    public RlpxAgent build() {
      validate();

      if (connectionEvents == null) {
        connectionEvents = new PeerConnectionEvents(metricsSystem);
      }
      if (connectionInitializer == null) {
        connectionInitializer =
            new NettyConnectionInitializer(
                keyPair, config, localNode, connectionEvents, metricsSystem);
      }

      final PeerRlpxPermissions rlpxPermissions =
          new PeerRlpxPermissions(localNode, peerPermissions);
      return new RlpxAgent(
          localNode,
          rlpxPermissions,
          connectionEvents,
          connectionInitializer,
          config.getMaxPeers(),
          maintainedPeers,
          metricsSystem);
    }

    private void validate() {
      checkState(keyPair != null, "KeyPair must be configured");
      checkState(localNode != null, "LocalNode must be configured");
      checkState(config != null, "RlpxConfiguration must be set");
      checkState(maintainedPeers != null, "MaintainedPeers must be configured");
      checkState(peerPermissions != null, "PeerPermissions must be configured");
      checkState(metricsSystem != null, "MetricsSystem must be configured");
    }

    public Builder keyPair(final KeyPair keyPair) {
      checkNotNull(keyPair);
      this.keyPair = keyPair;
      return this;
    }

    public Builder localNode(final LocalNode localNode) {
      checkNotNull(localNode);
      this.localNode = localNode;
      return this;
    }

    public Builder connectionInitializer(final ConnectionInitializer connectionInitializer) {
      checkNotNull(connectionInitializer);
      this.connectionInitializer = connectionInitializer;
      return this;
    }

    public Builder config(final RlpxConfiguration config) {
      checkNotNull(config);
      this.config = config;
      return this;
    }

    public Builder maintainedPeers(final MaintainedPeers maintainedPeers) {
      checkNotNull(maintainedPeers);
      this.maintainedPeers = maintainedPeers;
      return this;
    }

    public Builder peerPermissions(final PeerPermissions peerPermissions) {
      checkNotNull(peerPermissions);
      this.peerPermissions = peerPermissions;
      return this;
    }

    public Builder connectionEvents(final PeerConnectionEvents connectionEvents) {
      checkNotNull(connectionEvents);
      this.connectionEvents = connectionEvents;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      checkNotNull(metricsSystem);
      this.metricsSystem = metricsSystem;
      return this;
    }
  }
}
