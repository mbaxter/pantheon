///*
// * Copyright 2019 ConsenSys AG.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// * specific language governing permissions and limitations under the License.
// */
//package tech.pegasys.pantheon.ethereum.p2p.network;
//
//import static com.google.common.base.Preconditions.checkNotNull;
//import static org.mockito.ArgumentMatchers.anyInt;
//import static org.mockito.Mockito.lenient;
//import static org.mockito.Mockito.mock;
//
//import tech.pegasys.pantheon.crypto.SECP256K1;
//import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
//import tech.pegasys.pantheon.ethereum.chain.Blockchain;
//import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
//import tech.pegasys.pantheon.ethereum.p2p.config.DiscoveryConfiguration;
//import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
//import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
//import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryAgent;
//import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
//import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
//import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
//import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
//import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
//import tech.pegasys.pantheon.metrics.MetricsSystem;
//import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
//import tech.pegasys.pantheon.util.bytes.BytesValue;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Optional;
//import java.util.concurrent.CompletableFuture;
//import java.util.function.Function;
//import java.util.stream.Stream;
//
//public class MockP2PNetwork extends {
//  boolean started = false;
//
//  public MockP2PNetwork(
//      final KeyPair keyPair,
//      final NetworkingConfiguration config,
//      final List<Capability> supportedCapabilities,
//      final PeerBlacklist peerBlacklist,
//      final MetricsSystem metricsSystem,
//      final Optional<NodePermissioningController> nodePermissioningController,
//      final Blockchain blockchain) {
//    super(
//        getPeerDiscoveryAgent(keyPair, config),
//        keyPair,
//        config,
//        supportedCapabilities,
//        peerBlacklist,
//        metricsSystem,
//        nodePermissioningController,
//        blockchain);
//  }
//
//  public static MockP2PNetworkBuilder builder() {
//    return new MockP2PNetworkBuilder();
//  }
//
//  public static MockP2PNetwork create() {
//    return builder().build();
//  }
//
//  public static MockPeerConnection createMockPeerConnection(final Peer peer) {
//    return MockPeerConnection.create(peer);
//  }
//
//  private static PeerDiscoveryAgent getPeerDiscoveryAgent(
//      final KeyPair keyPair, final NetworkingConfiguration config) {
//    BytesValue peerId = keyPair.getPublicKey().getEncodedBytes();
//    Peer peer =
//        new DefaultPeer(
//            peerId,
//            config.getDiscovery().getAdvertisedHost(),
//            config.getDiscovery().getBindPort(),
//            config.getRlpx().getBindPort());
//
//    PeerDiscoveryAgent agent = mock(PeerDiscoveryAgent.class);
//    lenient().when(agent.getAdvertisedPeer()).thenReturn(Optional.of(peer));
//    lenient().when(agent.getId()).thenReturn(peerId);
//    lenient().when(agent.start(anyInt())).thenReturn(CompletableFuture.completedFuture(null));
//    lenient().when(agent.stop()).thenReturn(CompletableFuture.completedFuture(null));
//    lenient().when(agent.getPeers()).thenReturn(Stream.empty());
//
//    return agent;
//  }
//
//  @Override
//  protected CompletableFuture<Void> stopListening() {
//    started = false;
//    return CompletableFuture.completedFuture(null);
//  }
//
//  @Override
//  protected CompletableFuture<Integer> startListening(
//      final RlpxConfiguration config, final List<Capability> supportedCapabilities) {
//    started = true;
//    return CompletableFuture.completedFuture(config.getBindPort());
//  }
//
//  public void simulateIncomingConnection(final Peer peer) {
//    PeerConnection incomingConnection = createDefaultConnection(peer);
//    handleIncomingConnection(incomingConnection);
//  }
//
//  @Override
//  protected CompletableFuture<PeerConnection> initiateConnection(final Peer peer) {
//    return CompletableFuture.completedFuture(createDefaultConnection(peer));
//  }
//
//  private PeerConnection createDefaultConnection(final Peer peer) {
//    return MockPeerConnection.create(peer, this);
//  }
//
//  public static class MockP2PNetworkBuilder {
//    private NodePermissioningController nodePermissioningController = null;
//    private Blockchain blockchain = null;
//    private KeyPair keyPair = SECP256K1.KeyPair.generate();
//    private DiscoveryConfiguration discoveryConfiguration =
//        DiscoveryConfiguration.create().setActive(false);
//    private RlpxConfiguration rlpxConfiguration = RlpxConfiguration.create();
//    private List<Capability> capabilities = Arrays.asList(Capability.create("eth", 63));
//
//    private MockP2PNetworkBuilder() {}
//
//    public MockP2PNetworkBuilder keyPair(final KeyPair keyPair) {
//      this.keyPair = keyPair;
//      return this;
//    }
//
//    public MockP2PNetworkBuilder rlpxConfig(final RlpxConfiguration rlpxConfiguration) {
//      this.rlpxConfiguration = rlpxConfiguration;
//      return this;
//    }
//
//    public MockP2PNetworkBuilder rlpxConfig(
//        final Function<RlpxConfiguration, RlpxConfiguration> configModifier) {
//      return rlpxConfig(configModifier.apply(this.rlpxConfiguration));
//    }
//
//    MockP2PNetwork build() {
//      checkNotNull(keyPair);
//      checkNotNull(discoveryConfiguration);
//      checkNotNull(rlpxConfiguration);
//
//      final NetworkingConfiguration networkingConfiguration =
//          NetworkingConfiguration.create()
//              .setDiscovery(discoveryConfiguration)
//              .setRlpx(rlpxConfiguration);
//
//      return new MockP2PNetwork(
//          keyPair,
//          networkingConfiguration,
//          capabilities,
//          new PeerBlacklist(),
//          new NoOpMetricsSystem(),
//          Optional.ofNullable(nodePermissioningController),
//          blockchain);
//    }
//  }
//}
