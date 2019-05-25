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

import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.p2p.api.ConnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.Message;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class RlpxAgent {
  private static final Logger LOG = LogManager.getLogger();

  protected final KeyPair keyPair;
  protected final LocalNode localNode;
  protected final RlpxConfiguration config;

  protected Subscribers<ConnectCallback> connectSubscribers = new Subscribers<>();
  protected Subscribers<DisconnectCallback> disconnectSubscribers = new Subscribers<>();
  protected Map<Capability, Subscribers<MessageCallback>> messageSubscribers =
      new ConcurrentHashMap<>();

  private AtomicBoolean started = new AtomicBoolean();
  private AtomicBoolean stopped = new AtomicBoolean();

  private final LabelledMetric<Counter> disconnectCounter;
  private final Counter connectedPeersCounter;

  protected RlpxAgent(
      final KeyPair keyPair,
      final RlpxConfiguration config,
      final LocalNode localNode,
      final MetricsSystem metricsSystem) {
    this.keyPair = keyPair;
    this.config = config;
    this.localNode = localNode;

    // Setup metrics
    disconnectCounter =
        metricsSystem.createLabelledCounter(
            MetricCategory.PEERS,
            "disconnected_total",
            "Total number of peers disconnected",
            "initiator",
            "disconnectReason");

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

  public CompletableFuture<Integer> start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "Unable to start and already started " + getClass().getSimpleName());
    }
    return startListening();
  }

  public CompletableFuture<Void> stop() {
    if (!started.get() || !stopped.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }

    streamConnections().forEach((conn) -> conn.disconnect(DisconnectReason.CLIENT_QUITTING));
    return stopListening();
  }

  public abstract Stream<? extends PeerConnection> streamConnections();

  public abstract int getConnectionCount();

  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    if (!localNode.isReady()) {
      final CompletableFuture<PeerConnection> future = new CompletableFuture<>();
      Exception err =
          new IllegalStateException(
              "Cannot connect before "
                  + this.getClass().getSimpleName()
                  + " has finished starting");
      future.completeExceptionally(err);
      return future;
    }

    final EnodeURL enode = peer.getEnodeURL();
    if (!enode.isListening()) {
      final CompletableFuture<PeerConnection> future = new CompletableFuture<>();
      final String errorMsg =
          "Attempt to connect to peer with no listening port: " + enode.toString();
      LOG.warn(errorMsg);
      future.completeExceptionally(new IllegalArgumentException(errorMsg));
      return future;
    }

    LOG.trace("Initiating connection to peer: {}", enode);
    if (peer instanceof DiscoveryPeer) {
      ((DiscoveryPeer) peer).setLastAttemptedConnection(System.currentTimeMillis());
    }
    return doConnect(peer)
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

  protected abstract CompletableFuture<PeerConnection> doConnect(final Peer peer);

  protected abstract CompletableFuture<Integer> startListening();

  protected abstract CompletableFuture<Void> stopListening();

  public void subscribeMessage(final Capability capability, final MessageCallback callback) {
    messageSubscribers.computeIfAbsent(capability, key -> new Subscribers<>()).subscribe(callback);
  }

  public void subscribeConnect(final ConnectCallback callback) {
    connectSubscribers.subscribe(callback);
  }

  public void subscribeDisconnect(final DisconnectCallback callback) {
    disconnectSubscribers.subscribe(callback);
  }

  protected void dispatchConnect(final PeerConnection connection) {
    connectedPeersCounter.inc();
    connectSubscribers.forEach(c -> c.onConnect(connection));
  }

  protected void dispatchDisconnect(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    disconnectCounter.labels(initiatedByPeer ? "remote" : "local", reason.name()).inc();
    disconnectSubscribers.forEach(s -> s.onDisconnect(connection, reason, initiatedByPeer));
  }

  protected void dispatchMessage(final Capability capability, final Message message) {
    messageSubscribers
        .getOrDefault(capability, Subscribers.none())
        .forEach(s -> s.onMessage(capability, message));
  }
}
