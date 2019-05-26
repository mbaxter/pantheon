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
package tech.pegasys.pantheon.ethereum.p2p.network;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.api.ConnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryEvent.PeerBondedEvent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.VertxPeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.MaintainedPeers;
import tech.pegasys.pantheon.ethereum.p2p.peers.MutableLocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PeerPermissionsBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.RlpxAgent;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.PeerRlpxPermissions;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The peer network service (defunct PeerNetworkingService) is the entrypoint to the peer-to-peer
 * components of the Ethereum client. It implements the devp2p framework from the Ethereum
 * specifications.
 *
 * <p>This component manages the peer discovery layer, the RLPx wire protocol and the subprotocols
 * supported by this client.
 *
 * <h2>Peer Discovery</h2>
 *
 * Ethereum nodes discover one another via a simple UDP protocol that follows some of the techniques
 * described in the Kademlia DHT paper. Particularly nodes are classified in a k-bucket table
 * composed of 256 buckets, where each bucket contains at most 16 peers whose <i>XOR(SHA3(x))</i>
 * distance from us is equal to the index of the bucket. The value <i>x</i> in the distance function
 * corresponds to our node ID (public key).
 *
 * <p>Upper layers in the stack subscribe to events from the peer discovery layer and initiate/drop
 * connections accordingly.
 *
 * <h2>RLPx Wire Protocol</h2>
 *
 * The RLPx wire protocol is responsible for selecting peers to engage with, authenticating and
 * encrypting communications with peers, multiplexing subprotocols, framing messages, controlling
 * legality of messages, keeping connections alive, and keeping track of peer reputation.
 *
 * <h2>Subprotocols</h2>
 *
 * Subprotocols are pluggable elements on top of the RLPx framework, which can handle a specific set
 * of messages. Each subprotocol has a 3-char ASCII denominator and a version number, and statically
 * defines a count of messages it can handle.
 *
 * <p>The RLPx wire protocol dispatches messages to subprotocols based on the capabilities agreed by
 * each of the two peers during the protocol handshake.
 *
 * @see <a href="https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf">Kademlia DHT
 *     paper</a>
 * @see <a href="https://github.com/ethereum/wiki/wiki/Kademlia-Peer-Selection">Kademlia Peer
 *     Selection</a>
 * @see <a href="https://github.com/ethereum/devp2p/blob/master/rlpx.md">devp2p RLPx</a>
 */
public class DefaultP2PNetwork implements P2PNetwork {

  private static final Logger LOG = LogManager.getLogger();

  private final ScheduledExecutorService peerConnectionScheduler =
      Executors.newSingleThreadScheduledExecutor();
  private final PeerDiscoveryAgent peerDiscoveryAgent;
  private final RlpxAgent rlpxAgent;

  private final NetworkingConfiguration config;
  private final int maxPeers;

  private final SECP256K1.KeyPair keyPair;
  private final BytesValue nodeId;
  private final MutableLocalNode localNode;

  private final PeerRlpxPermissions peerPermissions;

  private final MaintainedPeers maintainedPeers;

  final Map<Peer, CompletableFuture<PeerConnection>> pendingConnections = new ConcurrentHashMap<>();
  private OptionalLong peerBondedObserverId = OptionalLong.empty();

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private CountDownLatch shutdownLatch = new CountDownLatch(2);
  private final Duration shutdownTimeout = Duration.ofMinutes(1);

  /**
   * Creates a peer networking service for production purposes.
   *
   * <p>The caller is expected to provide the IP address to be advertised (normally this node's
   * public IP address), as well as TCP and UDP port numbers for the RLPx agent and the discovery
   * agent, respectively.
   *
   * @param localNode A representation of the local node
   * @param peerDiscoveryAgent The agent responsible for discovering peers on the network.
   * @param keyPair This node's keypair.
   * @param config The network configuration to use.
   * @param peerPermissions An object that determines whether peers are allowed to connect
   */
  DefaultP2PNetwork(
      final MutableLocalNode localNode,
      final PeerDiscoveryAgent peerDiscoveryAgent,
      final RlpxAgent rlpxAgent,
      final SECP256K1.KeyPair keyPair,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final MaintainedPeers maintainedPeers,
      final PeerReputationManager reputationManager) {

    this.localNode = localNode;
    this.peerDiscoveryAgent = peerDiscoveryAgent;
    this.rlpxAgent = rlpxAgent;
    this.keyPair = keyPair;
    this.config = config;
    this.maintainedPeers = maintainedPeers;

    this.nodeId = this.keyPair.getPublicKey().getEncodedBytes();
    this.maxPeers = config.getRlpx().getMaxPeers();

    this.peerPermissions = new PeerRlpxPermissions(localNode, peerPermissions);

    peerDiscoveryAgent.addPeerRequirement(() -> rlpxAgent.getConnectionCount() >= maxPeers);

    subscribeDisconnect(reputationManager);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      LOG.warn("Attempted to start an already started " + getClass().getSimpleName());
    }

    this.peerPermissions.subscribeUpdate(this::handlePermissionsUpdate);
    this.maintainedPeers.subscribeAdd(this::handleMaintainedPeerAdded);
    this.maintainedPeers.subscribeRemove(this::handleMaintainedPeerRemoved);

    final int listeningPort = rlpxAgent.start().join();
    final int discoveryPort = peerDiscoveryAgent.start(listeningPort).join();
    setLocalNode(listeningPort, discoveryPort);

    peerBondedObserverId =
        OptionalLong.of(peerDiscoveryAgent.observePeerBondedEvents(this::handlePeerBondedEvent));

    peerConnectionScheduler.scheduleWithFixedDelay(
        this::checkMaintainedConnectionPeers, 2, 60, TimeUnit.SECONDS);
    peerConnectionScheduler.scheduleWithFixedDelay(
        this::attemptPeerConnections, 30, 30, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    if (!this.started.get() || !stopped.compareAndSet(false, true)) {
      // We haven't started, or we've started and stopped already
      return;
    }

    peerConnectionScheduler.shutdownNow();
    peerDiscoveryAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
    rlpxAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
    peerBondedObserverId.ifPresent(peerDiscoveryAgent::removePeerBondedObserver);
    peerBondedObserverId = OptionalLong.empty();
    peerPermissions.close();
  }

  @Override
  public void awaitStop() {
    try {
      peerConnectionScheduler.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);
      shutdownLatch.await(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);
    } catch (final InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public boolean addMaintainConnectionPeer(final Peer peer) {
    return maintainedPeers.add(peer);
  }

  @Override
  public boolean removeMaintainedConnectionPeer(final Peer peer) {
    return maintainedPeers.remove(peer);
  }

  void checkMaintainedConnectionPeers() {
    if (!localNode.isReady()) {
      return;
    }
    maintainedPeers.streamPeers().forEach(this::connect);
  }

  @VisibleForTesting
  // TODO
  void attemptPeerConnections() {
    //    if (!localNode.isReady()) {
    //      return;
    //    }
    //    final int availablePeerSlots = Math.max(0, maxPeers - connectionCount());
    //    if (availablePeerSlots <= 0) {
    //      return;
    //    }
    //
    //    final List<DiscoveryPeer> peers =
    //        streamDiscoveredPeers()
    //            .filter(peer -> peer.getStatus() == PeerDiscoveryStatus.BONDED)
    //            .filter(peerPermissions::allowNewOutboundConnectionTo)
    //            .filter(peer -> !isConnected(peer) && !isConnecting(peer))
    //            .sorted(Comparator.comparing(DiscoveryPeer::getLastAttemptedConnection))
    //            .collect(Collectors.toList());
    //
    //    if (peers.size() == 0) {
    //      return;
    //    }
    //
    //    LOG.trace(
    //        "Initiating connection to {} peers from the peer table",
    //        Math.min(availablePeerSlots, peers.size()));
    //    peers.stream().limit(availablePeerSlots).forEach(this::connect);
  }

  @VisibleForTesting
  int connectionCount() {
    return pendingConnections.size() + rlpxAgent.getConnectionCount();
  }

  @Override
  public Collection<PeerConnection> getPeers() {
    return rlpxAgent.streamConnections().collect(Collectors.toList());
  }

  @Override
  public Stream<DiscoveryPeer> streamDiscoveredPeers() {
    return peerDiscoveryAgent.streamDiscoveredPeers();
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
    if (!localNode.isReady()) {
      connectionFuture.completeExceptionally(
          new IllegalStateException("Attempt to connect to peer before network is ready"));
      return connectionFuture;
    }
    if (!peerPermissions.allowNewOutboundConnectionTo(peer)) {
      // Peer not allowed
      connectionFuture.completeExceptionally(
          new IllegalStateException("Unable to connect to disallowed peer: " + peer));
      return connectionFuture;
    }

    // TODO
    // Check for existing connection
    //    final Optional<PeerConnection> existingConnection =
    //        connections.getConnectionForPeer(peer.getId());
    //    if (existingConnection.isPresent()) {
    //      connectionFuture.complete(existingConnection.get());
    //      return connectionFuture;
    //    }
    //    // Check for existing pending connection
    //    final CompletableFuture<PeerConnection> existingPendingConnection =
    //        pendingConnections.putIfAbsent(peer, connectionFuture);
    //    if (existingPendingConnection != null) {
    //      return existingPendingConnection;
    //    }

    return rlpxAgent.connect(peer);
  }

  @Override
  public void subscribe(final Capability capability, final MessageCallback callback) {
    rlpxAgent.subscribeMessage(capability, callback);
  }

  @Override
  public void subscribeConnect(final ConnectCallback callback) {
    rlpxAgent.subscribeConnect(callback);
  }

  @Override
  public void subscribeDisconnect(final DisconnectCallback callback) {
    rlpxAgent.subscribeDisconnect(callback);
  }

  private void handleMaintainedPeerRemoved(final Peer peer, final boolean wasRemoved) {
    // Drop peer from peer table
    peerDiscoveryAgent.dropPeer(peer);

    // TODO
    // Disconnect if connected or connecting
    //    final CompletableFuture<PeerConnection> connectionFuture = pendingConnections.get(peer);
    //    if (connectionFuture != null) {
    //      connectionFuture.thenAccept(connection ->
    // connection.disconnect(DisconnectReason.REQUESTED));
    //    }
    //    final Optional<PeerConnection> peerConnection =
    // connections.getConnectionForPeer(peer.getId());
    //    peerConnection.ifPresent(pc -> pc.disconnect(DisconnectReason.REQUESTED));
  }

  private void handleMaintainedPeerAdded(final Peer peer, final boolean wasAdded) {
    this.connect(peer);
  }

  @VisibleForTesting
  // TODO
  void handlePeerBondedEvent(final PeerBondedEvent peerBondedEvent) {
    //    return event -> {
    //      final Peer peer = event.getPeer();
    //      if (connectionCount() < maxPeers && !isConnecting(peer) && !isConnected(peer)) {
    //        connect(peer);
    //      }
    //    };
  }

  private void handlePermissionsUpdate(
      final boolean permissionsRestricted, final Optional<List<Peer>> peers) {
    if (!permissionsRestricted) {
      // Nothing to do
      return;
    }

    // TODO
    //    if (peers.isPresent()) {
    //      peers.get().stream()
    //          .filter(p -> !peerPermissions.allowOngoingConnection(p))
    //          .map(Peer::getId)
    //          .map(connections::getConnectionForPeer)
    //          .filter(Optional::isPresent)
    //          .map(Optional::get)
    //          .forEach(conn -> conn.disconnect(DisconnectReason.REQUESTED));
    //    } else {
    //      checkAllConnections();
    //    }
  }
  //
  //  private void checkAllConnections() {
  //    connections
  //        .getPeerConnections()
  //        .forEach(
  //            peerConnection -> {
  //              final Peer peer = peerConnection.getPeer();
  //              if (!peerPermissions.allowOngoingConnection(peer)) {
  //                peerConnection.disconnect(DisconnectReason.REQUESTED);
  //              }
  //            });
  //  }

  // TODO
  //  @VisibleForTesting
  //  boolean isConnecting(final Peer peer) {
  //    return pendingConnections.containsKey(peer);
  //  }
  //
  //  @VisibleForTesting
  //  boolean isConnected(final Peer peer) {
  //    return connections.isAlreadyConnected(peer.getId());
  //  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public boolean isListening() {
    return peerDiscoveryAgent.isActive();
  }

  @Override
  public boolean isP2pEnabled() {
    return true;
  }

  @Override
  public boolean isDiscoveryEnabled() {
    return config.getDiscovery().isActive();
  }

  @Override
  public Optional<EnodeURL> getLocalEnode() {
    if (!localNode.isReady()) {
      return Optional.empty();
    }
    return Optional.of(localNode.getPeer().getEnodeURL());
  }

  private void setLocalNode(final int listeningPort, final int discoveryPort) {
    if (localNode.isReady()) {
      // Already set
      return;
    }

    final EnodeURL localEnode =
        EnodeURL.builder()
            .nodeId(nodeId)
            .ipAddress(config.getDiscovery().getAdvertisedHost())
            .listeningPort(listeningPort)
            .discoveryPort(discoveryPort)
            .build();

    LOG.info("Enode URL {}", localEnode.toString());
    localNode.setEnode(localEnode);
  }

  //  private void onConnectionEstablished(final PeerConnection connection) {
  //    connections.registerConnection(connection);
  //    connectCallbacks.forEach(callback -> callback.accept(connection));
  //  }

  public static class Builder {

    private PeerDiscoveryAgent peerDiscoveryAgent;
    private RlpxAgent rlpxAgent;
    private KeyPair keyPair;
    private NetworkingConfiguration config = NetworkingConfiguration.create();
    private List<Capability> supportedCapabilities;
    private PeerPermissions peerPermissions = PeerPermissions.noop();
    private MetricsSystem metricsSystem;
    private Optional<NodePermissioningController> nodePermissioningController = Optional.empty();
    private Blockchain blockchain = null;
    private Vertx vertx;
    private MaintainedPeers maintainedPeers = new MaintainedPeers();

    public P2PNetwork build() {
      validate();
      return doBuild();
    }

    private P2PNetwork doBuild() {
      // Fold NodePermissioningController into peerPermissions
      if (nodePermissioningController.isPresent()) {
        final List<EnodeURL> bootnodes = config.getDiscovery().getBootnodes();
        final PeerPermissions nodePermissions =
            new NodePermissioningAdapter(nodePermissioningController.get(), bootnodes, blockchain);
        peerPermissions = PeerPermissions.combine(peerPermissions, nodePermissions);
      }

      final PeerPermissionsBlacklist misbehavingPeers = PeerPermissionsBlacklist.create(500);
      final PeerReputationManager reputationManager = new PeerReputationManager(misbehavingPeers);
      peerPermissions = PeerPermissions.combine(peerPermissions, misbehavingPeers);

      MutableLocalNode localNode =
          MutableLocalNode.create(config.getRlpx().getClientId(), 5, supportedCapabilities);
      peerDiscoveryAgent = peerDiscoveryAgent == null ? createDiscoveryAgent() : peerDiscoveryAgent;
      rlpxAgent = createRlpxAgent(localNode);

      return new DefaultP2PNetwork(
          localNode,
          peerDiscoveryAgent,
          rlpxAgent,
          keyPair,
          config,
          peerPermissions,
          maintainedPeers,
          reputationManager);
    }

    private void validate() {
      checkState(keyPair != null, "KeyPair must be set.");
      checkState(config != null, "NetworkingConfiguration must be set.");
      checkState(
          supportedCapabilities != null && supportedCapabilities.size() > 0,
          "Supported capabilities must be set and non-empty.");
      checkState(metricsSystem != null, "MetricsSystem must be set.");
      checkState(
          !nodePermissioningController.isPresent() || blockchain != null,
          "Network permissioning needs to listen to BlockAddedEvents. Blockchain can't be null.");
      checkState(vertx != null, "Vertx must be set.");
    }

    private PeerDiscoveryAgent createDiscoveryAgent() {

      return new VertxPeerDiscoveryAgent(
          vertx, keyPair, config.getDiscovery(), peerPermissions, metricsSystem);
    }

    private RlpxAgent createRlpxAgent(final LocalNode localNode) {
      return RlpxAgent.builder()
          .keyPair(keyPair)
          .config(config.getRlpx())
          .localNode(localNode)
          .metricsSystem(metricsSystem)
          .build();
    }

    public Builder vertx(final Vertx vertx) {
      checkNotNull(vertx);
      this.vertx = vertx;
      return this;
    }

    public Builder keyPair(final KeyPair keyPair) {
      checkNotNull(keyPair);
      this.keyPair = keyPair;
      return this;
    }

    public Builder config(final NetworkingConfiguration config) {
      checkNotNull(config);
      this.config = config;
      return this;
    }

    public Builder supportedCapabilities(final List<Capability> supportedCapabilities) {
      checkNotNull(supportedCapabilities);
      this.supportedCapabilities = supportedCapabilities;
      return this;
    }

    public Builder supportedCapabilities(final Capability... supportedCapabilities) {
      this.supportedCapabilities = Arrays.asList(supportedCapabilities);
      return this;
    }

    public Builder peerPermissions(final PeerPermissions peerPermissions) {
      checkNotNull(peerPermissions);
      this.peerPermissions = peerPermissions;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      checkNotNull(metricsSystem);
      this.metricsSystem = metricsSystem;
      return this;
    }

    public Builder nodePermissioningController(
        final NodePermissioningController nodePermissioningController) {
      this.nodePermissioningController = Optional.ofNullable(nodePermissioningController);
      return this;
    }

    public Builder nodePermissioningController(
        final Optional<NodePermissioningController> nodePermissioningController) {
      this.nodePermissioningController = nodePermissioningController;
      return this;
    }

    public Builder blockchain(final Blockchain blockchain) {
      this.blockchain = blockchain;
      return this;
    }

    public Builder maintainedPeers(final MaintainedPeers maintainedPeers) {
      checkNotNull(maintainedPeers);
      this.maintainedPeers = maintainedPeers;
      return this;
    }
  }
}
