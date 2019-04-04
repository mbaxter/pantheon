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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.chain.BlockAddedEvent;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryEvent.PeerBondedEvent;
import tech.pegasys.pantheon.ethereum.p2p.netty.P2PNetworkTestHelper.Disconnection;
import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.DuplicatePeerConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.TooManyPeersConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Endpoint;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public final class AbstractP2PNetworkTest {

  //  @Mock private Blockchain blockchain;
  //
  //  private ArgumentCaptor<BlockAddedObserver> observerCaptor =
  //      ArgumentCaptor.forClass(BlockAddedObserver.class);

  //  @Before
  //  public void before() {
  //    when(blockchain.observeBlockAdded(observerCaptor.capture())).thenReturn(1L);
  //  }

  @Test
  public void preventMultipleConnections() throws Exception {
    final Peer peer = new DefaultPeer(Peer.randomId(), "127.0.0.1", 30303);
    try (final P2PNetwork network = MockP2PNetwork.create()) {
      network.start();

      final CompletableFuture<PeerConnection> connectFuture1 = network.connect(peer);
      assertThat(connectFuture1.get(30L, TimeUnit.SECONDS).getPeer()).isEqualTo(peer);

      final CompletableFuture<PeerConnection> secondConnectionFuture = network.connect(peer);

      assertThatThrownBy(secondConnectionFuture::get)
          .hasCauseInstanceOf(DuplicatePeerConnectionException.class);
    }
  }

  @Test
  public void limitOutgoingConnections() throws Exception {
    final int maxPeers = 1;
    final Peer peer1 = new DefaultPeer(Peer.randomId(), "127.0.0.1", 30303);
    final Peer peer2 = new DefaultPeer(Peer.randomId(), "127.0.0.1", 30303);

    try (final P2PNetwork network =
        MockP2PNetwork.builder().rlpxConfig((config) -> config.setMaxPeers(maxPeers)).build()) {
      network.start();

      final CompletableFuture<PeerConnection> connectFuture1 = network.connect(peer1);
      assertThat(connectFuture1.get(30L, TimeUnit.SECONDS).getPeer()).isEqualTo(peer1);

      final CompletableFuture<PeerConnection> secondConnectionFuture = network.connect(peer2);

      assertThatThrownBy(secondConnectionFuture::get)
          .hasCauseInstanceOf(TooManyPeersConnectionException.class);
    }
  }

  @Test
  public void limitIncomingConnections() throws Exception {
    final int maxPeers = 1;
    final Peer peer1 = spy(new DefaultPeer(Peer.randomId(), "127.0.0.1", 30303));
    final Peer peer2 = spy(new DefaultPeer(Peer.randomId(), "127.0.0.1", 30303));

    try (final MockP2PNetwork network =
        MockP2PNetwork.builder().rlpxConfig((config) -> config.setMaxPeers(maxPeers)).build()) {
      network.start();

      List<Disconnection> disconnections = P2PNetworkTestHelper.collectDisconnects(network);
      final CompletableFuture<PeerConnection> connectFuture1 = network.connect(peer1);
      assertThat(connectFuture1.get(30L, TimeUnit.SECONDS).getPeer()).isEqualTo(peer1);
      assertThat(disconnections.size()).isEqualTo(0);

      network.simulateIncomingConnection(peer2);

      // One of the peers should be disconnected with a too many peers message
      assertThat(disconnections.size()).isEqualTo(1);
      Disconnection disconnection = disconnections.get(0);
      assertThat(disconnection.getReason()).isEqualTo(DisconnectReason.TOO_MANY_PEERS);
      Peer disconnectedPeer = disconnection.getConnection().getPeer();
      assertThat(disconnectedPeer.equals(peer1) || disconnectedPeer.equals(peer2)).isTrue();
    }
  }

  //  @Test
  //  public void rejectIncomingConnectionFromBlacklistedPeer() throws Exception {
  //    final DiscoveryConfiguration noDiscovery = DiscoveryConfiguration.create().setActive(false);
  //    final SECP256K1.KeyPair localKp = SECP256K1.KeyPair.generate();
  //    final SECP256K1.KeyPair remoteKp = SECP256K1.KeyPair.generate();
  //    final BytesValue localId = localKp.getPublicKey().getEncodedBytes();
  //    final BytesValue remoteId = remoteKp.getPublicKey().getEncodedBytes();
  //    final PeerBlacklist localBlacklist = new PeerBlacklist();
  //    final PeerBlacklist remoteBlacklist = new PeerBlacklist();
  //
  //    final SubProtocol subprotocol = subProtocol();
  //    final Capability cap = Capability.create(subprotocol.getName(), 63);
  //    try (final P2PNetwork localNetwork =
  //            new NettyP2PNetwork(
  //                vertx,
  //                localKp,
  //                NetworkingConfiguration.create()
  //                    .setDiscovery(noDiscovery)
  //                    .setSupportedProtocols(subprotocol)
  //                    .setRlpx(RlpxConfiguration.create().setBindPort(0)),
  //                singletonList(cap),
  //                localBlacklist,
  //                new NoOpMetricsSystem(),
  //                Optional.empty(),
  //                Optional.empty());
  //        final P2PNetwork remoteNetwork =
  //            new NettyP2PNetwork(
  //                vertx,
  //                remoteKp,
  //                NetworkingConfiguration.create()
  //                    .setSupportedProtocols(subprotocol)
  //                    .setRlpx(RlpxConfiguration.create().setBindPort(0))
  //                    .setDiscovery(noDiscovery),
  //                singletonList(cap),
  //                remoteBlacklist,
  //                new NoOpMetricsSystem(),
  //                Optional.empty(),
  //                Optional.empty())) {
  //      final int localListenPort = localNetwork.getLocalPeerInfo().getPort();
  //      final int remoteListenPort = remoteNetwork.getLocalPeerInfo().getPort();
  //      final Peer localPeer =
  //          new DefaultPeer(
  //              localId,
  //              new Endpoint(
  //                  InetAddress.getLoopbackAddress().getHostAddress(),
  //                  localListenPort,
  //                  OptionalInt.of(localListenPort)));
  //
  //      final Peer remotePeer =
  //          new DefaultPeer(
  //              remoteId,
  //              new Endpoint(
  //                  InetAddress.getLoopbackAddress().getHostAddress(),
  //                  remoteListenPort,
  //                  OptionalInt.of(remoteListenPort)));
  //
  //      // Blacklist the remote peer
  //      localBlacklist.add(remotePeer);
  //
  //      localNetwork.start();
  //      remoteNetwork.start();
  //
  //      // Setup disconnect listener
  //      final CompletableFuture<PeerConnection> peerFuture = new CompletableFuture<>();
  //      final CompletableFuture<DisconnectReason> reasonFuture = new CompletableFuture<>();
  //      remoteNetwork.subscribeDisconnect(
  //          (peerConnection, reason, initiatedByPeer) -> {
  //            peerFuture.complete(peerConnection);
  //            reasonFuture.complete(reason);
  //          });
  //
  //      // Remote connect to local
  //      final CompletableFuture<PeerConnection> connectFuture = remoteNetwork.connect(localPeer);
  //
  //      // Check connection is made, and then a disconnect is registered at remote
  //      assertThat(connectFuture.get(5L, TimeUnit.SECONDS).getPeerInfo().getNodeId())
  //          .isEqualTo(localId);
  //      assertThat(peerFuture.get(5L,
  // TimeUnit.SECONDS).getPeerInfo().getNodeId()).isEqualTo(localId);
  //      assertThat(reasonFuture.get(5L, TimeUnit.SECONDS))
  //          .isEqualByComparingTo(DisconnectReason.UNKNOWN);
  //    }
  //  }
  //
  //  @Test
  //  public void rejectIncomingConnectionFromNonWhitelistedPeer() throws Exception {
  //    final DiscoveryConfiguration noDiscovery = DiscoveryConfiguration.create().setActive(false);
  //    final SECP256K1.KeyPair localKp = SECP256K1.KeyPair.generate();
  //    final SECP256K1.KeyPair remoteKp = SECP256K1.KeyPair.generate();
  //    final BytesValue localId = localKp.getPublicKey().getEncodedBytes();
  //    final PeerBlacklist localBlacklist = new PeerBlacklist();
  //    final PeerBlacklist remoteBlacklist = new PeerBlacklist();
  //    final LocalPermissioningConfiguration config =
  // LocalPermissioningConfiguration.createDefault();
  //    final Path tempFile = Files.createTempFile("test", "test");
  //    tempFile.toFile().deleteOnExit();
  //    config.setNodePermissioningConfigFilePath(tempFile.toAbsolutePath().toString());
  //
  //    final NodeLocalConfigPermissioningController localWhitelistController =
  //        new NodeLocalConfigPermissioningController(config, Collections.emptyList(), selfEnode);
  //    // turn on whitelisting by adding a different node NOT remote node
  //    localWhitelistController.addNodes(Arrays.asList(mockPeer().getEnodeURLString()));
  //    final NodePermissioningController nodePermissioningController =
  //        new NodePermissioningController(
  //            Optional.empty(), Collections.singletonList(localWhitelistController));
  //
  //    final SubProtocol subprotocol = subProtocol();
  //    final Capability cap = Capability.create(subprotocol.getName(), 63);
  //    try (final P2PNetwork localNetwork =
  //            new NettyP2PNetwork(
  //                vertx,
  //                localKp,
  //                NetworkingConfiguration.create()
  //                    .setDiscovery(noDiscovery)
  //                    .setSupportedProtocols(subprotocol)
  //                    .setRlpx(RlpxConfiguration.create().setBindPort(0)),
  //                singletonList(cap),
  //                localBlacklist,
  //                new NoOpMetricsSystem(),
  //                Optional.of(localWhitelistController),
  //                Optional.of(nodePermissioningController),
  //                blockchain);
  //        final P2PNetwork remoteNetwork =
  //            new NettyP2PNetwork(
  //                vertx,
  //                remoteKp,
  //                NetworkingConfiguration.create()
  //                    .setSupportedProtocols(subprotocol)
  //                    .setRlpx(RlpxConfiguration.create().setBindPort(0))
  //                    .setDiscovery(noDiscovery),
  //                singletonList(cap),
  //                remoteBlacklist,
  //                new NoOpMetricsSystem(),
  //                Optional.empty(),
  //                Optional.empty())) {
  //      final int localListenPort = localNetwork.getLocalPeerInfo().getPort();
  //      final Peer localPeer =
  //          new DefaultPeer(
  //              localId,
  //              new Endpoint(
  //                  InetAddress.getLoopbackAddress().getHostAddress(),
  //                  localListenPort,
  //                  OptionalInt.of(localListenPort)));
  //
  //      localNetwork.start();
  //      remoteNetwork.start();
  //
  //      // Setup disconnect listener
  //      final CompletableFuture<PeerConnection> peerFuture = new CompletableFuture<>();
  //      final CompletableFuture<DisconnectReason> reasonFuture = new CompletableFuture<>();
  //      remoteNetwork.subscribeDisconnect(
  //          (peerConnection, reason, initiatedByPeer) -> {
  //            peerFuture.complete(peerConnection);
  //            reasonFuture.complete(reason);
  //          });
  //
  //      // Remote connect to local
  //      final CompletableFuture<PeerConnection> connectFuture = remoteNetwork.connect(localPeer);
  //
  //      // Check connection is made, and then a disconnect is registered at remote
  //      assertThat(connectFuture.get(5L, TimeUnit.SECONDS).getPeerInfo().getNodeId())
  //          .isEqualTo(localId);
  //      assertThat(peerFuture.get(5L,
  // TimeUnit.SECONDS).getPeerInfo().getNodeId()).isEqualTo(localId);
  //      assertThat(reasonFuture.get(5L, TimeUnit.SECONDS))
  //          .isEqualByComparingTo(DisconnectReason.UNKNOWN);
  //    }
  //  }
  //
  //  private SubProtocol subProtocol() {
  //    return new SubProtocol() {
  //      @Override
  //      public String getName() {
  //        return "eth";
  //      }
  //
  //      @Override
  //      public int messageSpace(final int protocolVersion) {
  //        return 8;
  //      }
  //
  //      @Override
  //      public boolean isValidMessageCode(final int protocolVersion, final int code) {
  //        return true;
  //      }
  //
  //      @Override
  //      public String messageName(final int protocolVersion, final int code) {
  //        return INVALID_MESSAGE_NAME;
  //      }
  //    };
  //  }
  //
  //  private SubProtocol subProtocol2() {
  //    return new SubProtocol() {
  //      @Override
  //      public String getName() {
  //        return "ryj";
  //      }
  //
  //      @Override
  //      public int messageSpace(final int protocolVersion) {
  //        return 8;
  //      }
  //
  //      @Override
  //      public boolean isValidMessageCode(final int protocolVersion, final int code) {
  //        return true;
  //      }
  //
  //      @Override
  //      public String messageName(final int protocolVersion, final int code) {
  //        return INVALID_MESSAGE_NAME;
  //      }
  //    };
  //  }
  //
  //  @Test
  //  public void shouldSendClientQuittingWhenNetworkStops() {
  //    final NettyP2PNetwork network = mockP2PNetwork();
  //    final Peer peer = mockPeer();
  //    final PeerConnection peerConnection = mockPeerConnection();
  //
  //    network.start();
  //    network.connect(peer).complete(peerConnection);
  //    network.stop();
  //
  //    verify(peerConnection).disconnect(eq(DisconnectReason.CLIENT_QUITTING));
  //  }
  //
  //  @Test
  //  public void shouldntAttemptNewConnectionToPendingPeer() {
  //    final NettyP2PNetwork network = mockP2PNetwork();
  //    final Peer peer = mockPeer();
  //
  //    final CompletableFuture<PeerConnection> connectingFuture = network.connect(peer);
  //    assertThat(network.connect(peer)).isEqualTo(connectingFuture);
  //  }
  //
  //  @Test
  //  public void whenStartingNetworkWithNodePermissioningShouldSubscribeToBlockAddedEvents() {
  //    final NettyP2PNetwork network = mockP2PNetwork();
  //
  //    network.start();
  //
  //    verify(blockchain).observeBlockAdded(any());
  //  }
  //
  //  @Test
  //  public void whenStartingNetworkWithNodePermissioningWithoutBlockchainShouldThrowIllegalState()
  // {
  //    blockchain = null;
  //    final NettyP2PNetwork network = mockP2PNetwork();
  //
  //    final Throwable throwable = catchThrowable(network::start);
  //    assertThat(throwable)
  //        .isInstanceOf(IllegalStateException.class)
  //        .hasMessage(
  //            "NettyP2PNetwork permissioning needs to listen to BlockAddedEvents. Blockchain can't
  // be null.");
  //  }
  //
  //  @Test
  //  public void whenStoppingNetworkWithNodePermissioningShouldUnsubscribeBlockAddedEvents() {
  //    final NettyP2PNetwork network = mockP2PNetwork();
  //
  //    network.start();
  //    network.stop();
  //
  //    verify(blockchain).removeObserver(eq(1L));
  //  }
  //
  //  @Test
  //  public void onBlockAddedShouldCheckPermissionsForAllPeers() {
  //    final BlockAddedEvent blockAddedEvent = blockAddedEvent();
  //    final NettyP2PNetwork network = mockP2PNetwork();
  //    final Peer localPeer = mockPeer("127.0.0.1", 30301);
  //    final Peer remotePeer1 = mockPeer("127.0.0.2", 30302);
  //    final Peer remotePeer2 = mockPeer("127.0.0.3", 30303);
  //
  //    final PeerConnection peerConnection1 = mockPeerConnection(localPeer, remotePeer1);
  //    final PeerConnection peerConnection2 = mockPeerConnection(localPeer, remotePeer2);
  //
  //    network.start();
  //    network.connect(remotePeer1).complete(peerConnection1);
  //    network.connect(remotePeer2).complete(peerConnection2);
  //
  //    final BlockAddedObserver blockAddedObserver = observerCaptor.getValue();
  //    blockAddedObserver.onBlockAdded(blockAddedEvent, blockchain);
  //
  //    verify(nodePermissioningController, times(2)).isPermitted(any(), any());
  //  }
  //
  //  @Test
  //  public void onBlockAddedAndPeerNotPermittedShouldDisconnect() {
  //    final BlockAddedEvent blockAddedEvent = blockAddedEvent();
  //    final NettyP2PNetwork network = mockP2PNetwork();
  //
  //    final Peer localPeer = mockPeer("127.0.0.1", 30301);
  //    final Peer permittedPeer = mockPeer("127.0.0.2", 30302);
  //    final Peer notPermittedPeer = mockPeer("127.0.0.3", 30303);
  //
  //    final PeerConnection permittedPeerConnection = mockPeerConnection(localPeer, permittedPeer);
  //    final PeerConnection notPermittedPeerConnection =
  //        mockPeerConnection(localPeer, notPermittedPeer);
  //
  //    final EnodeURL permittedEnodeURL = EnodeURL.fromString(permittedPeer.getEnodeURLString());
  //    final EnodeURL notPermittedEnodeURL =
  // EnodeURL.fromString(notPermittedPeer.getEnodeURLString());
  //
  //    network.start();
  //    network.connect(permittedPeer).complete(permittedPeerConnection);
  //    network.connect(notPermittedPeer).complete(notPermittedPeerConnection);
  //
  //    reset(nodePermissioningController);
  //
  //    lenient()
  //        .when(nodePermissioningController.isPermitted(any(), enodeEq(notPermittedEnodeURL)))
  //        .thenReturn(false);
  //    lenient()
  //        .when(nodePermissioningController.isPermitted(any(), enodeEq(permittedEnodeURL)))
  //        .thenReturn(true);
  //
  //    final BlockAddedObserver blockAddedObserver = observerCaptor.getValue();
  //    blockAddedObserver.onBlockAdded(blockAddedEvent, blockchain);
  //
  //    verify(notPermittedPeerConnection).disconnect(eq(DisconnectReason.REQUESTED));
  //    verify(permittedPeerConnection, never()).disconnect(any());
  //  }

  @Test
  public void beforeStartingNetworkEnodeURLShouldNotBePresent() {
    final MockP2PNetwork network = MockP2PNetwork.create();

    assertThat(network.getSelfEnodeURL()).isNotPresent();
  }

  @Test
  public void afterStartingNetworkEnodeURLShouldBePresent() {
    final MockP2PNetwork network = MockP2PNetwork.create();
    network.start();

    assertThat(network.getSelfEnodeURL()).isPresent();
  }

  @Test
  public void handlePeerBondedEvent_forPeerWithNoTcpPort() {
    DiscoveryPeer peer = new DiscoveryPeer(Peer.randomId(), "127.0.0.1", 999, OptionalInt.empty());
    AtomicInteger connectCount = new AtomicInteger(0);

    final MockP2PNetwork network = MockP2PNetwork.create();
    network.subscribeConnect(
        (PeerConnection conn) -> {
          if (conn.getPeer().getId().equals(peer.getId())) {
            connectCount.incrementAndGet();
          }
        });
    network.start();

    PeerBondedEvent peerBondedEvent = new PeerBondedEvent(peer, System.currentTimeMillis());

    network.handlePeerBondedEvent().accept(peerBondedEvent);
    assertThat(connectCount.get()).isEqualTo(1);
  }

  private BytesValue generatePeerId(final int seed) {
    BlockDataGenerator gen = new BlockDataGenerator(seed);
    return gen.bytesValue(DefaultPeer.PEER_ID_SIZE);
  }

  private BlockAddedEvent blockAddedEvent() {
    return mock(BlockAddedEvent.class);
  }

  private PeerConnection mockPeerConnection(final BytesValue id) {
    final PeerInfo peerInfo = mock(PeerInfo.class);
    when(peerInfo.getNodeId()).thenReturn(id);
    final PeerConnection peerConnection = mock(PeerConnection.class);
    when(peerConnection.getPeerInfo()).thenReturn(peerInfo);
    return peerConnection;
  }

  private PeerConnection mockPeerConnection() {
    return mockPeerConnection(BytesValue.fromHexString("0x00"));
  }

  private PeerConnection mockPeerConnection(final Peer localPeer, final Peer remotePeer) {
    final PeerInfo peerInfo = mock(PeerInfo.class);
    doReturn(remotePeer.getId()).when(peerInfo).getNodeId();
    doReturn(remotePeer.getEndpoint().getTcpPort().getAsInt()).when(peerInfo).getPort();

    final PeerConnection peerConnection = mock(PeerConnection.class);
    when(peerConnection.getPeerInfo()).thenReturn(peerInfo);

    Endpoint localEndpoint = localPeer.getEndpoint();
    InetSocketAddress localSocketAddress =
        new InetSocketAddress(localEndpoint.getHost(), localEndpoint.getTcpPort().getAsInt());
    when(peerConnection.getLocalAddress()).thenReturn(localSocketAddress);

    Endpoint remoteEndpoint = remotePeer.getEndpoint();
    InetSocketAddress remoteSocketAddress =
        new InetSocketAddress(remoteEndpoint.getHost(), remoteEndpoint.getTcpPort().getAsInt());
    when(peerConnection.getRemoteAddress()).thenReturn(remoteSocketAddress);

    return peerConnection;
  }

  //  private NettyP2PNetwork mockNettyP2PNetwork() {
  //    return mockNettyP2PNetwork(RlpxConfiguration::create);
  //  }
  //
  //  private NettyP2PNetwork mockNettyP2PNetwork(final Supplier<RlpxConfiguration> rlpxConfig) {
  //    NettyP2PNetwork network = spy(mockP2PNetwork(rlpxConfig));
  //    lenient().doReturn(new CompletableFuture<>()).when(network).connect(any());
  //    return network;
  //  }

  //  private MockP2PNetwork mockP2PNetwork(final Supplier<RlpxConfiguration> rlpxConfig) {
  //    final DiscoveryConfiguration noDiscovery = DiscoveryConfiguration.create().setActive(false);
  //    final SECP256K1.KeyPair keyPair = SECP256K1.KeyPair.generate();
  //    final Capability cap = Capability.create("eth", 63);
  //    final NetworkingConfiguration networkingConfiguration =
  //        NetworkingConfiguration.create()
  //            .setDiscovery(noDiscovery)
  //            .setSupportedProtocols(subProtocol())
  //            .setRlpx(rlpxConfig.get().setBindPort(0));
  //
  //    lenient().when(nodePermissioningController.isPermitted(any(), any())).thenReturn(true);
  //
  //    return new MockP2PNetwork(
  //        keyPair,
  //        networkingConfiguration,
  //        singletonList(cap),
  //        new PeerBlacklist(),
  //        new NoOpMetricsSystem(),
  //        Optional.of(nodePermissioningController));
  //  }

  private Peer mockPeer() {
    return P2PNetworkTestHelper.mockPeer();
  }

  private Peer mockPeer(final String host, final int port) {
    return P2PNetworkTestHelper.mockPeer(host, port);
  }

  public static class EnodeURLMatcher implements ArgumentMatcher<EnodeURL> {

    private final EnodeURL enodeURL;

    EnodeURLMatcher(final EnodeURL enodeURL) {
      this.enodeURL = enodeURL;
    }

    @Override
    public boolean matches(final EnodeURL argument) {
      if (argument == null) {
        return false;
      } else {
        return enodeURL.getNodeId().equals(argument.getNodeId())
            && enodeURL.getIp().equals(argument.getIp())
            && enodeURL.getListeningPort().equals(argument.getListeningPort());
      }
    }
  }

  private static NodePermissioningController mockNodePermissioningController() {
    NodePermissioningController nodePermissioningController =
        mock(NodePermissioningController.class);
    lenient().when(nodePermissioningController.isPermitted(any(), any())).thenReturn(true);
    return nodePermissioningController;
  }

  private EnodeURL enodeEq(final EnodeURL enodeURL) {
    return argThat(new EnodeURLMatcher(enodeURL));
  }
}
