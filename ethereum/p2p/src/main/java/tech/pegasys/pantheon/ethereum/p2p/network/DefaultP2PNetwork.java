package tech.pegasys.pantheon.ethereum.p2p.network;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import io.vertx.core.Vertx;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.api.Message;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.VertxPeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.exceptions.P2PNetworkNotReadyException;
import tech.pegasys.pantheon.ethereum.p2p.network.permissions.DefaultPeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.network.permissions.NodeLocalConfigPeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.network.permissions.NodePermissioningControllerPeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultLocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.MutableLocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissions;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerPermissionsBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.RlpxAgent;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.ConnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.MessageCallback;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.ConnectingToLocalNodeException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.netty.NettyRlpxAgent;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.MaintainedPeers;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.PeerConnectionManager;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.DefaultMessage;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.permissioning.NodeLocalConfigPermissioningController;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

public class DefaultP2PNetwork implements P2PNetwork {
  private static final Logger LOG = LogManager.getLogger();
  private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(1);

  private final ScheduledExecutorService peerConnectionScheduler =
    Executors.newSingleThreadScheduledExecutor();

  private final KeyPair keyPair;
  private final BytesValue nodeId;
  private final MutableLocalNode localNode;
  private final PeerDiscoveryAgent discoveryAgent;
  private final RlpxAgent rlpxAgent;

  private final NetworkingConfiguration config;
  private final MaintainedPeers maintainedPeers;
  private final DefaultPeerPermissions peerPermissions;
  private final PeerReputationManager peerReputationManager;

  private final PeerConnectionManager peerConnectionManager;
  private final Duration startupTimeout = DEFAULT_TIMEOUT;
  private final Duration shutdownTimeout = DEFAULT_TIMEOUT;

  private List<Runnable> shutdownCommands = new ArrayList<>();
  private CountDownLatch shutdownLatch = new CountDownLatch(2);
  private AtomicBoolean started = new AtomicBoolean(false);
  private AtomicBoolean stopped = new AtomicBoolean(false);

  public DefaultP2PNetwork(
    final KeyPair keyPair,
    final NetworkingConfiguration config,
    final MutableLocalNode localNode,
    final PeerDiscoveryAgent discoveryAgent,
    final RlpxAgent rlpxAgent,
    final MaintainedPeers maintainedPeers,
    final DefaultPeerPermissions peerPermissions) {

    this.config = config;
    this.keyPair = keyPair;
    this.localNode = localNode;
    this.discoveryAgent = discoveryAgent;
    this.rlpxAgent = rlpxAgent;
    this.maintainedPeers = maintainedPeers;
    this.peerPermissions = peerPermissions;

    this.nodeId = keyPair.getPublicKey().getEncodedBytes();
    this.peerConnectionManager = new PeerConnectionManager(localNode, rlpxAgent, maintainedPeers, peerPermissions, config.getRlpx().getMaxPeers());
    this.peerReputationManager = new PeerReputationManager(peerPermissions.getMisbehavingNodes());

    final int maxPeers = config.getRlpx().getMaxPeers();
    discoveryAgent.addPeerRequirement(
      () -> rlpxAgent.getConnections().count() >= maxPeers);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      try {
        this.setupListeners();

        final int listeningPort = rlpxAgent.start()
          .get(startupTimeout.getSeconds(), TimeUnit.SECONDS);
        discoveryAgent.start(listeningPort).get(startupTimeout.getSeconds(), TimeUnit.SECONDS);
        final int discoveryPort = discoveryAgent.getDiscoveryPort();

        final String host = config.getDiscovery().getAdvertisedHost();
        final EnodeURL enode = new EnodeURL(nodeId, host, listeningPort, discoveryPort);
        final Peer ourNode = DefaultPeer.fromEnodeURL(enode);
        localNode.set(ourNode);

        this.scheduleConnectionChecks();

      } catch (InterruptedException e) {
        throw new RuntimeException("Timed out while waiting for network startup", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Failed to startup network", e);
      } catch (TimeoutException e) {
        throw new RuntimeException("Timed out while waiting for network startup");
      }
    }
  }

  @Override
  public Collection<PeerConnection> getPeers() {
    return rlpxAgent.getConnections().collect(Collectors.toList());
  }

  @Override
  public Stream<DiscoveryPeer> getDiscoveredPeers() {
    return discoveryAgent.getPeers();
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    return peerConnectionManager.maybeConnect(peer);
  }

  @Override
  public void subscribe(final Capability capability, final Consumer<Message> consumer) {
    // TODO: rework subscribe to take a {@link MessageCallback} argument
    MessageCallback callback = (final PeerConnection connection, final Capability cap, final MessageData messageData) -> {
      final Message msg = new DefaultMessage(connection, messageData);
      consumer.accept(msg);
    };
    rlpxAgent.subscribeMessage(capability, callback);
  }

  @Override
  public void subscribeConnect(final Consumer<PeerConnection> consumer) {
    // TODO: rework subscribe to take a {@link ConnectCallback} argument
    ConnectCallback callback = (final PeerConnection peer, final boolean initiatedByPeer) -> {
      consumer.accept(peer);
    };
    rlpxAgent.subscribeConnect(callback);
  }

  @Override
  public void subscribeDisconnect(final DisconnectCallback consumer) {
    rlpxAgent.subscribeDisconnect(consumer);
  }

  @Override
  public boolean addMaintainConnectionPeer(final Peer peer) {
    checkNotNull(peer);
    if (!localNode.isReady()) {
      throw new P2PNetworkNotReadyException();
    }
    if (Objects.equals(peer.getId(), localNode.getPeer().get().getId())) {
      throw new ConnectingToLocalNodeException();
    }
    return maintainedPeers.add(peer);
  }

  @Override
  public boolean removeMaintainedConnectionPeer(final Peer peer) {
    return maintainedPeers.remove(peer);
  }

  @Override
  public void stop() {
    if (started.get() && stopped.compareAndSet(false, true)) {
      shutdownCommands.forEach(Runnable::run);
      peerConnectionScheduler.shutdownNow();
      discoveryAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
      rlpxAgent.stop().whenComplete((res, err) -> shutdownLatch.countDown());
    }
  }

  @Override
  public void awaitStop() {
    try {
      peerConnectionScheduler.awaitTermination(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);
      shutdownLatch.await(shutdownTimeout.getSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Received interrupt while shutting down", e);
    }
  }

  @Override
  public Optional<? extends Peer> getAdvertisedPeer() {
    return discoveryAgent.getAdvertisedPeer();
  }

  @Override
  public PeerInfo getLocalPeerInfo() {
    return rlpxAgent.getPeerInfo();
  }

  @Override
  public boolean isListening() {
    return localNode.isReady();
  }

  @Override
  public boolean isP2pEnabled() {
    return true;
  }

  @Override
  public Optional<EnodeURL> getSelfEnodeURL() {
    return localNode.getPeer().map(Peer::getEnodeURL);
  }

  @Override
  public void close() throws IOException {
    stop();
  }

  private void setupListeners() {
    rlpxAgent.subscribeDisconnect(this.peerReputationManager);

    final long bondedId = discoveryAgent.observePeerBondedEvents((peerBondedEvent) ->
      peerConnectionManager.maybeConnect(peerBondedEvent.getPeer()));
    shutdownCommands.add(() -> discoveryAgent.removePeerBondedObserver(bondedId));

    final long droppedId = discoveryAgent.observePeerDroppedEvents((peerDroppedEvent ->
      peerConnectionManager.disconnect(peerDroppedEvent.getPeer())));
    shutdownCommands.add(() -> discoveryAgent.removePeerDroppedObserver(droppedId));
  }

  private void scheduleConnectionChecks() {
    peerConnectionScheduler.scheduleWithFixedDelay(
      peerConnectionManager::checkMaintainedPeers, 60, 60, TimeUnit.SECONDS);
    peerConnectionScheduler.scheduleWithFixedDelay(
      this::connectToAvailablePeers, 30, 30, TimeUnit.SECONDS);
  }

  private void connectToAvailablePeers() {
    peerConnectionManager.connectToAvailablePeers(this::getDiscoveredPeers);
  }

  public static class Builder {
    private KeyPair keyPair;
    private NetworkingConfiguration config = NetworkingConfiguration.create();
    private List<Capability> supportedCapabilities;
    private MetricsSystem metricsSystem;
    private List<BytesValue> bannedNodeIds = new ArrayList<>();
    private Optional<NodeLocalConfigPermissioningController>
      nodeLocalConfigPermissioningController = Optional.empty();
    private Optional<NodePermissioningController> nodePermissioningController = Optional.empty();
    private Blockchain blockchain = null;
    private Vertx vertx;

    // These values should be created based on the dependencies configured in the builder
    // and not set externally
    private DefaultPeerPermissions peerPermissions;
    private DefaultPeerPermissions discoveryPeerPermissions;
    private final MutableLocalNode localNode = new DefaultLocalNode();
    private PeerDiscoveryAgent peerDiscoveryAgent;
    private RlpxAgent rlpxAgent;
    private MaintainedPeers maintainedPeers;

    private Builder(){}

    public P2PNetwork build() {
      validate();

      // Setup permissions
      final PeerPermissionsBlacklist bannedNodes = PeerPermissionsBlacklist.create();
      bannedNodeIds.forEach(bannedNodes::add);
      final PeerPermissionsBlacklist misbehavingPeers = PeerPermissionsBlacklist.create(500);
      final List<PeerPermissions> otherPermissions = new ArrayList<>();
      nodePermissioningController.map(c -> new NodePermissioningControllerPeerPermissions(c, localNode, blockchain)).ifPresent(otherPermissions::add);
      final DefaultPeerPermissions peerPermissions = DefaultPeerPermissions.create(bannedNodes, misbehavingPeers, otherPermissions);

      // Specialize discovery permissions
      final List<PeerPermissions> otherDiscoveryPermissions = new ArrayList<>();
      nodeLocalConfigPermissioningController.map(c -> new NodeLocalConfigPeerPermissions(c)).ifPresent(otherDiscoveryPermissions::add);
      discoveryPeerPermissions = otherDiscoveryPermissions.isEmpty() ? peerPermissions :  DefaultPeerPermissions.create(bannedNodes, misbehavingPeers, otherDiscoveryPermissions);

      maintainedPeers = new MaintainedPeers(peerPermissions);

      return new DefaultP2PNetwork(
        keyPair,
        config,
        localNode,
        createDiscoveryAgent(),
        createRlpxAgent(),
        maintainedPeers,
        peerPermissions);
    }

    protected void validate() {
      checkNotNull(keyPair);
      checkNotNull(config);
      checkState(supportedCapabilities != null && supportedCapabilities.size() > 0);
      checkNotNull(metricsSystem);
      checkState(!nodePermissioningController.isPresent() || blockchain != null);
    }

    protected PeerDiscoveryAgent createDiscoveryAgent() {
      return new VertxPeerDiscoveryAgent(
        vertx,
        keyPair,
        config.getDiscovery(),
        discoveryPeerPermissions,
        metricsSystem);
    }

    private RlpxAgent createRlpxAgent() {
      return new NettyRlpxAgent(
      config,
      supportedCapabilities,
      config.getSupportedProtocols(),
      keyPair,
      metricsSystem
      );
    }

    public Builder vertx(final Vertx vertx) {
      this.vertx = vertx;
      return this;
    }

    public Builder nodeLocalConfigPermissioningController(
      final Optional<NodeLocalConfigPermissioningController>
        nodeLocalConfigPermissioningController) {
      this.nodeLocalConfigPermissioningController = nodeLocalConfigPermissioningController;
      return this;
    }

    public Builder nodeLocalConfigPermissioningController(
      final NodeLocalConfigPermissioningController nodeLocalConfigPermissioningController) {
      this.nodeLocalConfigPermissioningController =
        Optional.ofNullable(nodeLocalConfigPermissioningController);
      return this;
    }

    public Builder keyPair(final KeyPair keyPair) {
      this.keyPair = keyPair;
      return this;
    }

    public Builder config(final NetworkingConfiguration config) {
      this.config = config;
      return this;
    }

    public Builder supportedCapabilities(final List<Capability> supportedCapabilities) {
      this.supportedCapabilities = supportedCapabilities;
      return this;
    }

    public Builder supportedCapabilities(final Capability... supportedCapabilities) {
      this.supportedCapabilities = Arrays.asList(supportedCapabilities);
      return this;
    }

    public Builder banNodeIds(BytesValue... nodeIds) {
      return banNodeIds(Arrays.asList(nodeIds));
    }

    public Builder banNodeIds(Collection<BytesValue> nodeIds) {
      bannedNodeIds.addAll(nodeIds);
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
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
