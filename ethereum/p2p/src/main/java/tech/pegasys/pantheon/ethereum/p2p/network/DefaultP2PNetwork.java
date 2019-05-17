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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.api.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.Message;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryEvent.PeerBondedEvent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryStatus;
import tech.pegasys.pantheon.ethereum.p2p.discovery.VertxPeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.network.netty.Callbacks;
import tech.pegasys.pantheon.ethereum.p2p.network.netty.HandshakeHandlerInbound;
import tech.pegasys.pantheon.ethereum.p2p.network.netty.HandshakeHandlerOutbound;
import tech.pegasys.pantheon.ethereum.p2p.network.netty.PeerConnectionRegistry;
import tech.pegasys.pantheon.ethereum.p2p.network.netty.TimeoutHandler;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.permissions.PeerPermissionsBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.PeerRlpxPermissions;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.SubProtocol;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.SingleThreadEventExecutor;
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
  private static final int TIMEOUT_SECONDS = 30;

  private ChannelFuture server;
  private final EventLoopGroup boss = new NioEventLoopGroup(1);
  private final EventLoopGroup workers = new NioEventLoopGroup(1);
  private final ScheduledExecutorService peerConnectionScheduler =
      Executors.newSingleThreadScheduledExecutor();
  private final PeerDiscoveryAgent peerDiscoveryAgent;

  private final NetworkingConfiguration config;
  private final List<Capability> supportedCapabilities;
  private final int maxPeers;
  private final List<SubProtocol> subProtocols;

  private final SECP256K1.KeyPair keyPair;
  private final BytesValue nodeId;
  private volatile OptionalInt listeningPort = OptionalInt.empty();
  private volatile Optional<Peer> localPeer = Optional.empty();
  private volatile Optional<PeerInfo> ourPeerInfo = Optional.empty();

  private final PeerRlpxPermissions peerPermissions;

  @VisibleForTesting final Collection<Peer> peerMaintainConnectionList;
  @VisibleForTesting final PeerConnectionRegistry connections;

  @VisibleForTesting
  final Map<Peer, CompletableFuture<PeerConnection>> pendingConnections = new ConcurrentHashMap<>();

  final Map<Capability, Subscribers<Consumer<Message>>> protocolCallbacks =
      new ConcurrentHashMap<>();
  private final Subscribers<Consumer<PeerConnection>> connectCallbacks = new Subscribers<>();
  private final Subscribers<DisconnectCallback> disconnectCallbacks = new Subscribers<>();
  private final Callbacks callbacks = new Callbacks(protocolCallbacks, disconnectCallbacks);

  private final LabelledMetric<Counter> outboundMessagesCounter;
  private OptionalLong peerBondedObserverId = OptionalLong.empty();

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  /**
   * Creates a peer networking service for production purposes.
   *
   * <p>The caller is expected to provide the IP address to be advertised (normally this node's
   * public IP address), as well as TCP and UDP port numbers for the RLPx agent and the discovery
   * agent, respectively.
   *
   * @param peerDiscoveryAgent The agent responsible for discovering peers on the network.
   * @param keyPair This node's keypair.
   * @param config The network configuration to use.
   * @param supportedCapabilities The wire protocol capabilities to advertise to connected peers.
   * @param peerPermissions An object that determines whether peers are allowed to connect
   * @param metricsSystem The metrics system to capture metrics with.
   */
  DefaultP2PNetwork(
      final PeerDiscoveryAgent peerDiscoveryAgent,
      final SECP256K1.KeyPair keyPair,
      final NetworkingConfiguration config,
      final List<Capability> supportedCapabilities,
      final PeerPermissions peerPermissions,
      final MetricsSystem metricsSystem) {

    this.peerDiscoveryAgent = peerDiscoveryAgent;
    this.keyPair = keyPair;
    this.config = config;
    this.supportedCapabilities = supportedCapabilities;
    this.peerMaintainConnectionList = new HashSet<>();
    this.connections = new PeerConnectionRegistry(metricsSystem);

    this.nodeId = this.keyPair.getPublicKey().getEncodedBytes();
    this.subProtocols = config.getSupportedProtocols();
    this.maxPeers = config.getRlpx().getMaxPeers();

    // Set up permissions
    final PeerPermissionsBlacklist misbehavingPeers = PeerPermissionsBlacklist.create(500);
    PeerReputationManager reputationManager = new PeerReputationManager(misbehavingPeers);
    PeerPermissions.combine(peerPermissions, misbehavingPeers);
    this.peerPermissions = new PeerRlpxPermissions(() -> localPeer, peerPermissions);
    this.peerPermissions.subscribeUpdate(this::handlePermissionsUpdate);

    peerDiscoveryAgent.addPeerRequirement(() -> connections.size() >= maxPeers);

    subscribeDisconnect(reputationManager);
    subscribeDisconnect(connections);

    outboundMessagesCounter =
        metricsSystem.createLabelledCounter(
            MetricCategory.NETWORK,
            "p2p_messages_outbound",
            "Count of each P2P message sent outbound.",
            "protocol",
            "name",
            "code");

    metricsSystem.createIntegerGauge(
        MetricCategory.NETWORK,
        "netty_workers_pending_tasks",
        "The number of pending tasks in the Netty workers event loop",
        pendingTaskCounter(workers));

    metricsSystem.createIntegerGauge(
        MetricCategory.NETWORK,
        "netty_boss_pending_tasks",
        "The number of pending tasks in the Netty boss event loop",
        pendingTaskCounter(boss));
  }

  public static Builder builder() {
    return new Builder();
  }

  private Supplier<Integer> pendingTaskCounter(final EventLoopGroup eventLoopGroup) {
    return () ->
        StreamSupport.stream(eventLoopGroup.spliterator(), false)
            .filter(eventExecutor -> eventExecutor instanceof SingleThreadEventExecutor)
            .mapToInt(eventExecutor -> ((SingleThreadEventExecutor) eventExecutor).pendingTasks())
            .sum();
  }

  /**
   * Start listening for incoming connections.
   *
   * @return The port on which we're listening for incoming connections.
   */
  private int startListening() {
    server =
        new ServerBootstrap()
            .group(boss, workers)
            .channel(NioServerSocketChannel.class)
            .childHandler(inboundChannelInitializer())
            .bind(config.getRlpx().getBindHost(), config.getRlpx().getBindPort());
    final CountDownLatch latch = new CountDownLatch(1);
    server.addListener(
        future -> {
          final InetSocketAddress socketAddress =
              (InetSocketAddress) server.channel().localAddress();
          final String message =
              String.format(
                  "Unable start up P2P network on %s:%s.  Check for port conflicts.",
                  config.getRlpx().getBindHost(), config.getRlpx().getBindPort());

          if (!future.isSuccess()) {
            LOG.error(message, future.cause());
          }
          checkState(socketAddress != null, message);
          listeningPort = OptionalInt.of(socketAddress.getPort());
          ourPeerInfo =
              Optional.of(
                  new PeerInfo(
                      5,
                      config.getClientId(),
                      supportedCapabilities,
                      listeningPort.getAsInt(),
                      nodeId));
          LOG.info("P2PNetwork started and listening on {}", socketAddress);
          latch.countDown();
        });

    // Ensure ourPeerInfo has been set prior to returning
    try {
      if (!latch.await(1, TimeUnit.MINUTES)) {
        throw new RuntimeException("Timed out while waiting for network startup");
      }
      return listeningPort.getAsInt();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted before startup completed", e);
    }
  }

  /** @return a channel initializer for inbound connections */
  public ChannelInitializer<SocketChannel> inboundChannelInitializer() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(final SocketChannel ch) {
        final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
        ch.pipeline()
            .addLast(
                new TimeoutHandler<>(
                    connectionFuture::isDone,
                    TIMEOUT_SECONDS,
                    () ->
                        connectionFuture.completeExceptionally(
                            new TimeoutException(
                                "Timed out waiting to fully establish incoming connection"))),
                new HandshakeHandlerInbound(
                    keyPair,
                    subProtocols,
                    ourPeerInfo.get(),
                    connectionFuture,
                    callbacks,
                    connections,
                    outboundMessagesCounter));

        connectionFuture.thenAccept(
            connection -> {
              // Reject incoming connections if we've reached our limit
              if (connections.size() >= maxPeers) {
                LOG.debug(
                    "Disconnecting incoming connection because connection limit of {} has been reached: {}",
                    maxPeers,
                    connection.getPeerInfo().getNodeId());
                connection.disconnect(DisconnectReason.TOO_MANY_PEERS);
                return;
              }

              final Peer peer = connection.getPeer();
              if (!peerPermissions.allowNewInboundConnectionFrom(peer)) {
                connection.disconnect(DisconnectReason.UNKNOWN);
                return;
              }

              onConnectionEstablished(connection);
              LOG.debug(
                  "Successfully accepted connection from {}", connection.getPeerInfo().getNodeId());
              logConnections();
            });
      }
    };
  }

  @Override
  public boolean addMaintainConnectionPeer(final Peer peer) {
    checkArgument(
        peer.getEnodeURL().isListening(),
        "Invalid enode url.  Enode url must contain a non-zero listening port.");
    final boolean added = peerMaintainConnectionList.add(peer);
    final boolean allowConnection = peerPermissions.allowNewOutboundConnectionTo(peer);
    if (allowConnection && !isConnectingOrConnected(peer)) {
      // Connect immediately if appropriate
      connect(peer);
    }

    return added;
  }

  @Override
  public boolean removeMaintainedConnectionPeer(final Peer peer) {
    final boolean removed = peerMaintainConnectionList.remove(peer);

    final CompletableFuture<PeerConnection> connectionFuture = pendingConnections.get(peer);
    if (connectionFuture != null) {
      connectionFuture.thenAccept(connection -> connection.disconnect(DisconnectReason.REQUESTED));
    }

    final Optional<PeerConnection> peerConnection = connections.getConnectionForPeer(peer.getId());
    peerConnection.ifPresent(pc -> pc.disconnect(DisconnectReason.REQUESTED));

    peerDiscoveryAgent.dropPeer(peer);

    return removed;
  }

  void checkMaintainedConnectionPeers() {
    peerMaintainConnectionList.stream()
        .filter(p -> !isConnectingOrConnected(p))
        .filter(peer -> peerPermissions.allowNewOutboundConnectionTo(peer))
        .forEach(this::connect);
  }

  @VisibleForTesting
  void attemptPeerConnections() {
    final int availablePeerSlots = Math.max(0, maxPeers - connectionCount());
    if (availablePeerSlots <= 0) {
      return;
    }

    final List<DiscoveryPeer> peers =
        streamDiscoveredPeers()
            .filter(peer -> peer.getStatus() == PeerDiscoveryStatus.BONDED)
            .filter(peerPermissions::allowNewOutboundConnectionTo)
            .filter(peer -> !isConnected(peer) && !isConnecting(peer))
            .sorted(Comparator.comparing(DiscoveryPeer::getLastAttemptedConnection))
            .collect(Collectors.toList());

    if (peers.size() == 0) {
      return;
    }

    LOG.trace(
        "Initiating connection to {} peers from the peer table",
        Math.min(availablePeerSlots, peers.size()));
    peers.stream().limit(availablePeerSlots).forEach(this::connect);
  }

  @VisibleForTesting
  int connectionCount() {
    return pendingConnections.size() + connections.size();
  }

  @Override
  public Collection<PeerConnection> getPeers() {
    return connections.getPeerConnections();
  }

  @Override
  public Stream<DiscoveryPeer> streamDiscoveredPeers() {
    return peerDiscoveryAgent.streamDiscoveredPeers();
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
    if (!localPeer.isPresent()) {
      connectionFuture.completeExceptionally(
          new IllegalStateException("Attempt to connect to peer before network is ready"));
      return connectionFuture;
    }

    LOG.trace("Initiating connection to peer: {}", peer.getId());
    final CompletableFuture<PeerConnection> existingPendingConnection =
        pendingConnections.putIfAbsent(peer, connectionFuture);
    if (existingPendingConnection != null) {
      LOG.debug("Attempted to connect to peer with pending connection: {}", peer.getId());
      return existingPendingConnection;
    }
    final EnodeURL enode = peer.getEnodeURL();
    if (!enode.isListening()) {
      final String errorMsg =
          "Attempt to connect to peer with no listening port: " + enode.toString();
      LOG.warn(errorMsg);
      connectionFuture.completeExceptionally(new IllegalStateException(errorMsg));
      return connectionFuture;
    }

    if (peer instanceof DiscoveryPeer) {
      ((DiscoveryPeer) peer).setLastAttemptedConnection(System.currentTimeMillis());
    }
    new Bootstrap()
        .group(workers)
        .channel(NioSocketChannel.class)
        .remoteAddress(new InetSocketAddress(enode.getIp(), enode.getListeningPort().getAsInt()))
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, TIMEOUT_SECONDS * 1000)
        .handler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(final SocketChannel ch) {
                ch.pipeline()
                    .addLast(
                        new TimeoutHandler<>(
                            connectionFuture::isDone,
                            TIMEOUT_SECONDS,
                            () ->
                                connectionFuture.completeExceptionally(
                                    new TimeoutException(
                                        "Timed out waiting to establish connection with peer: "
                                            + peer.getId()))),
                        new HandshakeHandlerOutbound(
                            keyPair,
                            peer,
                            subProtocols,
                            ourPeerInfo.get(),
                            connectionFuture,
                            callbacks,
                            connections,
                            outboundMessagesCounter));
              }
            })
        .connect()
        .addListener(
            (f) -> {
              if (!f.isSuccess()) {
                connectionFuture.completeExceptionally(f.cause());
              }
            });

    connectionFuture.whenComplete(
        (connection, t) -> {
          pendingConnections.remove(peer);
          if (t == null) {
            onConnectionEstablished(connection);
            LOG.debug("Connection established to peer: {}", peer.getId());
          } else {
            LOG.debug("Failed to connect to peer {}: {}", peer.getId(), t);
          }
          logConnections();
        });
    return connectionFuture;
  }

  private void logConnections() {
    LOG.debug(
        "Connections: {} pending, {} active connections.",
        pendingConnections.size(),
        connections.size());
  }

  @Override
  public void subscribe(final Capability capability, final Consumer<Message> callback) {
    protocolCallbacks.computeIfAbsent(capability, key -> new Subscribers<>()).subscribe(callback);
  }

  @Override
  public void subscribeConnect(final Consumer<PeerConnection> callback) {
    connectCallbacks.subscribe(callback);
  }

  @Override
  public void subscribeDisconnect(final DisconnectCallback callback) {
    disconnectCallbacks.subscribe(callback);
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      LOG.warn("Attempted to start an already started " + getClass().getSimpleName());
    }

    final int listeningPort = startListening();
    peerDiscoveryAgent.start(listeningPort).join();
    peerBondedObserverId =
        OptionalLong.of(peerDiscoveryAgent.observePeerBondedEvents(handlePeerBondedEvent()));

    createLocalNode();

    peerConnectionScheduler.scheduleWithFixedDelay(
        this::checkMaintainedConnectionPeers, 2, 60, TimeUnit.SECONDS);
    peerConnectionScheduler.scheduleWithFixedDelay(
        this::attemptPeerConnections, 30, 30, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  Consumer<PeerBondedEvent> handlePeerBondedEvent() {
    return event -> {
      final Peer peer = event.getPeer();
      if (connectionCount() < maxPeers && !isConnecting(peer) && !isConnected(peer)) {
        connect(peer);
      }
    };
  }

  private void handlePermissionsUpdate(
      final boolean permissionsRestricted, final Optional<List<Peer>> peers) {
    if (!permissionsRestricted) {
      // Nothing to do
      return;
    }

    if (peers.isPresent()) {
      peers.get().stream()
          .filter(p -> !peerPermissions.allowOngoingConnection(p))
          .map(Peer::getId)
          .map(connections::getConnectionForPeer)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .forEach(conn -> conn.disconnect(DisconnectReason.REQUESTED));
    } else {
      checkAllConnections();
    }
  }

  private void checkAllConnections() {
    connections
        .getPeerConnections()
        .forEach(
            peerConnection -> {
              final Peer peer = peerConnection.getPeer();
              if (!peerPermissions.allowOngoingConnection(peer)) {
                peerConnection.disconnect(DisconnectReason.REQUESTED);
              }
            });
  }

  @VisibleForTesting
  boolean isConnecting(final Peer peer) {
    return pendingConnections.containsKey(peer);
  }

  @VisibleForTesting
  boolean isConnected(final Peer peer) {
    return connections.isAlreadyConnected(peer.getId());
  }

  private boolean isConnectingOrConnected(final Peer peer) {
    return isConnected(peer) || isConnecting(peer);
  }

  @Override
  public void stop() {
    if (!this.started.get() || !stopped.compareAndSet(false, true)) {
      // We haven't started, or we've started and stopped already
      return;
    }

    sendClientQuittingToPeers();
    peerConnectionScheduler.shutdownNow();
    peerDiscoveryAgent.stop().join();
    peerBondedObserverId.ifPresent(peerDiscoveryAgent::removePeerBondedObserver);
    peerBondedObserverId = OptionalLong.empty();
    peerDiscoveryAgent.stop().join();
    workers.shutdownGracefully();
    boss.shutdownGracefully();
  }

  private void sendClientQuittingToPeers() {
    connections.getPeerConnections().forEach(p -> p.disconnect(DisconnectReason.CLIENT_QUITTING));
  }

  @Override
  public void awaitStop() {
    try {
      server.channel().closeFuture().sync();
    } catch (final InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

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
    return localPeer.map(Peer::getEnodeURL);
  }

  private void createLocalNode() {
    if (localPeer.isPresent() || !listeningPort.isPresent()) {
      return;
    }

    final OptionalInt discoveryPort =
        peerDiscoveryAgent
            .getAdvertisedPeer()
            .map(Peer::getEnodeURL)
            .map(EnodeURL::getDiscoveryPort)
            .orElse(OptionalInt.empty());

    final EnodeURL localEnode =
        EnodeURL.builder()
            .nodeId(nodeId)
            .ipAddress(config.getDiscovery().getAdvertisedHost())
            .listeningPort(listeningPort.getAsInt())
            .discoveryPort(discoveryPort)
            .build();

    LOG.info("Enode URL {}", localEnode.toString());
    this.localPeer = Optional.of(DefaultPeer.fromEnodeURL(localEnode));
  }

  private void onConnectionEstablished(final PeerConnection connection) {
    connections.registerConnection(connection);
    connectCallbacks.forEach(callback -> callback.accept(connection));
  }

  public static class Builder {

    private PeerDiscoveryAgent peerDiscoveryAgent;
    private KeyPair keyPair;
    private NetworkingConfiguration config = NetworkingConfiguration.create();
    private List<Capability> supportedCapabilities;
    private PeerPermissions peerPermissions = PeerPermissions.noop();
    private MetricsSystem metricsSystem;
    private Optional<NodePermissioningController> nodePermissioningController = Optional.empty();
    private Blockchain blockchain = null;
    private Vertx vertx;

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
      peerDiscoveryAgent = peerDiscoveryAgent == null ? createDiscoveryAgent() : peerDiscoveryAgent;

      return new DefaultP2PNetwork(
          peerDiscoveryAgent,
          keyPair,
          config,
          supportedCapabilities,
          peerPermissions,
          metricsSystem);
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
  }
}
