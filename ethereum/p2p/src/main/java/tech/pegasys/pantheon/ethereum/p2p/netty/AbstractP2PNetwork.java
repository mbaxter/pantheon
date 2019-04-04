/*
 * Copyright 2018 ConsenSys AG.
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

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.api.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.Message;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryEvent.PeerBondedEvent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryEvent.PeerDroppedEvent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryStatus;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.PeerRequirement;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.DefaultMessage;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
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
public abstract class AbstractP2PNetwork implements P2PNetwork, PeerConnectionEventDispatcher {

  private static final Logger LOG = LogManager.getLogger();

  private final ScheduledExecutorService peerConnectionScheduler =
      Executors.newSingleThreadScheduledExecutor();

  private final Map<Capability, Subscribers<Consumer<Message>>> protocolCallbacks =
      new ConcurrentHashMap<>();
  private final Subscribers<Consumer<PeerConnection>> connectCallbacks = new Subscribers<>();
  private final Subscribers<DisconnectCallback> disconnectCallbacks = new Subscribers<>();

  private final PeerDiscoveryAgent peerDiscoveryAgent;
  private OptionalLong peerBondedObserverId = OptionalLong.empty();
  private OptionalLong peerDroppedObserverId = OptionalLong.empty();

  @VisibleForTesting final PeerConnectionManager peerConnectionManager;

  protected Optional<EnodeURL> ourEnodeURL = Optional.empty();
  protected PeerInfo ourPeerInfo;
  protected final List<Capability> ourCapabilities;
  protected final SECP256K1.KeyPair keyPair;

  private final int maxPeers;

  private final CountDownLatch stoppedCountDownLatch = new CountDownLatch(1);

  /**
   * Creates a peer networking service for production purposes.
   *
   * <p>The caller is expected to provide the IP address to be advertised (normally this node's
   * public IP address), as well as TCP and UDP port numbers for the RLPx agent and the discovery
   * agent, respectively.
   *
   * @param keyPair This node's keypair.
   * @param config The network configuration to use.
   * @param supportedCapabilities The wire protocol capabilities to advertise to connected peers.
   * @param peerBlacklist The peers with which this node will not connect
   * @param metricsSystem The metrics system to capture metrics with.
   * @param nodePermissioningController Controls node permissioning.
   * @param blockchain The blockchain to subscribe to BlockAddedEvents.
   */
  public AbstractP2PNetwork(
      final Function<PeerRequirement, PeerDiscoveryAgent> peerDiscoveryAgentSupplier,
      final SECP256K1.KeyPair keyPair,
      final NetworkingConfiguration config,
      final List<Capability> supportedCapabilities,
      final PeerBlacklist peerBlacklist,
      final MetricsSystem metricsSystem,
      final Optional<NodePermissioningController> nodePermissioningController,
      final Blockchain blockchain) {
    maxPeers = config.getRlpx().getMaxPeers();
    ourCapabilities = supportedCapabilities;
    this.peerConnectionManager =
        new PeerConnectionManager(
            this::initiateConnection,
            this::getDiscoveredPeers,
            maxPeers,
            peerBlacklist,
            metricsSystem);
    nodePermissioningController.ifPresent(
        c -> peerConnectionManager.setNodePermissioningController(c, blockchain));

    peerDiscoveryAgent =
        peerDiscoveryAgentSupplier.apply(
            () -> peerConnectionManager.countActiveConnections() >= maxPeers);

    subscribeDisconnect(peerBlacklist);

    this.keyPair = keyPair;

    CompletableFuture<Integer> listeningPortFuture =
        startListening(config.getRlpx(), supportedCapabilities);
    try {
      // Wait for network to initialize so we can set up our peer info accurately
      int listeningPort = listeningPortFuture.get(1, TimeUnit.MINUTES);
      ourPeerInfo =
          new PeerInfo(
              5,
              config.getClientId(),
              supportedCapabilities,
              listeningPort,
              this.keyPair.getPublicKey().getEncodedBytes());
    } catch (InterruptedException e) {
      throw new RuntimeException("Timed out while waiting for network startup", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to startup network", e);
    } catch (TimeoutException e) {
      throw new RuntimeException("Timed out while waiting for network startup");
    }
  }

  @Override
  public void start() {
    peerDiscoveryAgent.start(ourPeerInfo.getPort()).join();
    // We can only derive our enode after wire and discovery listeners have been set up and our
    // listening ports are fully determined
    this.ourEnodeURL = Optional.of(buildSelfEnodeURL());
    LOG.info("Enode URL {}", ourEnodeURL.toString());
    peerConnectionManager.setEnodeUrl(ourEnodeURL.get());
    peerConnectionManager.setEnabled();

    peerBondedObserverId =
        OptionalLong.of(peerDiscoveryAgent.observePeerBondedEvents(handlePeerBondedEvent()));
    peerDroppedObserverId =
        OptionalLong.of(peerDiscoveryAgent.observePeerDroppedEvents(handlePeerDroppedEvents()));

    peerConnectionScheduler.scheduleWithFixedDelay(
        peerConnectionManager::checkMaintainedPeers, 60, 60, TimeUnit.SECONDS);
    peerConnectionScheduler.scheduleWithFixedDelay(
        peerConnectionManager::connectToAvailablePeers, 30, 30, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    peerConnectionManager.shutdown();
    peerConnectionScheduler.shutdownNow();
    peerDiscoveryAgent.stop().join();
    peerBondedObserverId.ifPresent(peerDiscoveryAgent::removePeerBondedObserver);
    peerBondedObserverId = OptionalLong.empty();
    peerDroppedObserverId.ifPresent(peerDiscoveryAgent::removePeerDroppedObserver);
    peerDroppedObserverId = OptionalLong.empty();
    peerDiscoveryAgent.stop().join();
    stopListening().thenAccept((res) -> stoppedCountDownLatch.countDown());
  }

  protected abstract CompletableFuture<Void> stopListening();

  protected abstract CompletableFuture<Integer> startListening(
      final RlpxConfiguration config, final List<Capability> supportedCapabilities);

  protected void handleIncomingConnection(final PeerConnection incomingConnection) {
    if (peerConnectionManager.handleIncomingConnection(incomingConnection)) {
      handlePeerConnected(incomingConnection);
    }
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    return peerConnectionManager
        .maybeConnect(peer)
        .whenComplete(
            (connection, t) -> {
              if (t == null) {
                handlePeerConnected(connection);
                LOG.debug("Connection established to peer: {}", peer.getId());
              } else {
                LOG.debug("Failed to connect to peer {}: {}", peer.getId(), t);
              }
            });
  }

  protected abstract CompletableFuture<PeerConnection> initiateConnection(final Peer peer);

  @Override
  public boolean addMaintainConnectionPeer(final Peer peer) {
    return peerConnectionManager.addMaintainedPeer(peer);
  }

  @Override
  public boolean removeMaintainedConnectionPeer(final Peer peer) {
    return peerConnectionManager.removeMaintainedPeer(peer);
  }

  @Override
  public Collection<PeerConnection> getPeers() {
    return peerConnectionManager.getConnections().collect(Collectors.toList());
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

  @VisibleForTesting
  Consumer<PeerBondedEvent> handlePeerBondedEvent() {
    return event -> {
      connect(event.getPeer());
    };
  }

  private Consumer<PeerDroppedEvent> handlePeerDroppedEvents() {
    return event -> {
      final Peer peer = event.getPeer();
      getPeers().stream()
          .filter(p -> p.getPeerInfo().getNodeId().equals(peer.getId()))
          .findFirst()
          .ifPresent(p -> p.disconnect(DisconnectReason.REQUESTED));
    };
  }

  @Override
  public void awaitStop() {
    try {
      stoppedCountDownLatch.await();
    } catch (final InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public void close() {
    stop();
  }

  @VisibleForTesting
  public Stream<DiscoveryPeer> getDiscoveredPeers() {
    return peerDiscoveryAgent
        .getPeers()
        .filter(peer -> peer.getStatus() == PeerDiscoveryStatus.BONDED);
  }

  @Override
  public Optional<? extends Peer> getAdvertisedPeer() {
    return peerDiscoveryAgent.getAdvertisedPeer();
  }

  @Override
  public PeerInfo getLocalPeerInfo() {
    return ourPeerInfo;
  }

  @Override
  public boolean isListening() {
    return true;
  }

  @Override
  public boolean isP2pEnabled() {
    return true;
  }

  @Override
  public Optional<EnodeURL> getSelfEnodeURL() {
    return ourEnodeURL;
  }

  @Override
  public void dispatchPeerDisconnected(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initatedByPeer) {
    disconnectCallbacks.forEach(
        consumer -> consumer.onDisconnect(connection, reason, initatedByPeer));
    peerDiscoveryAgent.dropPeer(connection.getPeer().getId());
    peerConnectionManager.handleDisconnect(connection, reason, initatedByPeer);
  }

  @Override
  public void dispatchMessageReceived(
      final PeerConnection connection, final Capability capability, final MessageData message) {
    final Message fullMessage = new DefaultMessage(connection, message);
    protocolCallbacks
        .getOrDefault(capability, Subscribers.none())
        .forEach(
            consumer -> {
              try {
                consumer.accept(fullMessage);
              } catch (final Throwable t) {
                LOG.error("Error in callback:", t);
              }
            });
  }

  private void handlePeerConnected(final PeerConnection connection) {
    connectCallbacks.forEach(callback -> callback.accept(connection));
  }

  private EnodeURL buildSelfEnodeURL() {
    return peerDiscoveryAgent
        .getAdvertisedPeer()
        .map(Peer::getEnodeURL)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Attempt to build enode before discovery agent is ready."));
  }
}
