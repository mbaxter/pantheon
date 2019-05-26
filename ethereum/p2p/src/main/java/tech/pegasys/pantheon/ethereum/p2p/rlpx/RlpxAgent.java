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
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.ConnectionInitializer;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.PeerConnectionEvents;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.netty.NettyConnectionInitializer;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.FutureUtils;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RlpxAgent {
  private static final Logger LOG = LogManager.getLogger();

  private final LocalNode localNode;
  private final PeerConnectionEvents connectionEvents;
  private final Subscribers<ConnectCallback> connectSubscribers = Subscribers.create();
  private final ConnectionInitializer connectionInitializer;

  private final int maxPeers;
  private final Set<PeerConnection> connections = new HashSet<>();

  private AtomicBoolean started = new AtomicBoolean();
  private AtomicBoolean stopped = new AtomicBoolean();

  private final Counter connectedPeersCounter;

  private RlpxAgent(
      final LocalNode localNode,
      final PeerConnectionEvents connectionEvents,
      final ConnectionInitializer connectionInitializer,
      final int maxPeers,
      final MetricsSystem metricsSystem) {
    this.maxPeers = maxPeers;
    this.localNode = localNode;
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
    return connections.stream();
  }

  public int getConnectionCount() {
    return connections.size();
  }

  public CompletableFuture<PeerConnection> connect(final Peer peer) {
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
    // TODO - check for pending / existing connection

    LOG.trace("Initiating connection to peer: {}", enode);
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
              }
            });
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

      return new RlpxAgent(
          localNode, connectionEvents, connectionInitializer, config.getMaxPeers(), metricsSystem);
    }

    private void validate() {
      checkState(keyPair != null, "KeyPair must be configured");
      checkState(localNode != null, "LocalNode must be configured");
      checkState(config != null, "RlpxConfiguration must be set");
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
