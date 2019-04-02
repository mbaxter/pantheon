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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.DiscoveryConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.PeerRequirement;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.SubProtocol;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MockP2PNetwork extends AbstractP2PNetwork {
  boolean started = false;

  public MockP2PNetwork(
      final KeyPair keyPair,
      final NetworkingConfiguration config,
      final List<Capability> supportedCapabilities,
      final PeerBlacklist peerBlacklist,
      final MetricsSystem metricsSystem,
      final Optional<NodePermissioningController> nodePermissioningController,
      final Blockchain blockchain) {
    super(
        getPeerDiscoveryAgentSupplier(keyPair, config),
        keyPair,
        config,
        supportedCapabilities,
        peerBlacklist,
        metricsSystem,
        nodePermissioningController,
        blockchain);
  }

  public static MockP2PNetworkBuilder builder() {
    return new MockP2PNetworkBuilder();
  }

  public static MockP2PNetwork create() {
    return builder().build();
  }

  private static Function<PeerRequirement, PeerDiscoveryAgent> getPeerDiscoveryAgentSupplier(
      final KeyPair keyPair, final NetworkingConfiguration config) {
    BytesValue peerId = keyPair.getPublicKey().getEncodedBytes();
    Peer peer =
        new DefaultPeer(
            peerId,
            config.getDiscovery().getAdvertisedHost(),
            config.getDiscovery().getBindPort(),
            config.getRlpx().getBindPort());

    PeerDiscoveryAgent agent = mock(PeerDiscoveryAgent.class);
    lenient().when(agent.getAdvertisedPeer()).thenReturn(Optional.of(peer));
    lenient().when(agent.getId()).thenReturn(peerId);
    lenient().when(agent.start(anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    lenient().when(agent.stop()).thenReturn(CompletableFuture.completedFuture(null));
    lenient().when(agent.getPeers()).thenReturn(Stream.empty());

    return (PeerRequirement peerRequirement) -> agent;
  }

  @Override
  protected CompletableFuture<Void> stopListening() {
    started = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  protected CompletableFuture<Integer> startListening(
      final RlpxConfiguration config, final List<Capability> supportedCapabilities) {
    started = true;
    return CompletableFuture.completedFuture(config.getBindPort());
  }

  public void simulateIncomingConnection(final Peer peer) {
    PeerConnection incomingConnection = createDefaultConnection(peer);
    handleIncomingConnection(incomingConnection);
  }

  @Override
  protected CompletableFuture<PeerConnection> initiateConnection(final Peer peer) {
    return CompletableFuture.completedFuture(createDefaultConnection(peer));
  }

  private PeerConnection createDefaultConnection(final Peer peer) {
    return MockPeerConnection.create(peer, this);
  }

  public static class MockP2PNetworkBuilder {
    private NodePermissioningController nodePermissioningController = null;
    private Blockchain blockchain = null;
    private KeyPair keyPair = SECP256K1.KeyPair.generate();
    private DiscoveryConfiguration discoveryConfiguration =
        DiscoveryConfiguration.create().setActive(false);
    private RlpxConfiguration rlpxConfiguration = RlpxConfiguration.create();
    private List<SubProtocol> subprotocols = Arrays.asList(subProtocol());

    private MockP2PNetworkBuilder() {}

    public MockP2PNetworkBuilder keyPair(final KeyPair keyPair) {
      this.keyPair = keyPair;
      return this;
    }

    public MockP2PNetworkBuilder rlpxConfig(final RlpxConfiguration rlpxConfiguration) {
      this.rlpxConfiguration = rlpxConfiguration;
      return this;
    }

    public MockP2PNetworkBuilder rlpxConfig(
        final Function<RlpxConfiguration, RlpxConfiguration> configModifier) {
      this.rlpxConfiguration = configModifier.apply(this.rlpxConfiguration);
      return this;
    }

    MockP2PNetwork build() {
      checkNotNull(keyPair);
      checkNotNull(discoveryConfiguration);
      checkNotNull(rlpxConfiguration);
      checkNotNull(subprotocols);

      final NetworkingConfiguration networkingConfiguration =
          NetworkingConfiguration.create()
              .setDiscovery(discoveryConfiguration)
              .setSupportedProtocols(subprotocols)
              .setRlpx(rlpxConfiguration);

      return new MockP2PNetwork(
          keyPair,
          networkingConfiguration,
          getCapabilities(),
          new PeerBlacklist(),
          new NoOpMetricsSystem(),
          Optional.ofNullable(nodePermissioningController),
          blockchain);
    }

    private List<Capability> getCapabilities() {
      return subprotocols.stream()
          .map(s -> Capability.create(s.getName(), 1))
          .collect(Collectors.toList());
    }

    private static SubProtocol subProtocol() {
      return new SubProtocol() {
        @Override
        public String getName() {
          return "eth";
        }

        @Override
        public int messageSpace(final int protocolVersion) {
          return 8;
        }

        @Override
        public boolean isValidMessageCode(final int protocolVersion, final int code) {
          return true;
        }

        @Override
        public String messageName(final int protocolVersion, final int code) {
          return INVALID_MESSAGE_NAME;
        }
      };
    }
  }

  private static class MockPeerConnection extends AbstractPeerConnection {

    private MockPeerConnection(
        final Peer peer,
        final PeerInfo peerInfo,
        final InetSocketAddress localAddress,
        final InetSocketAddress remoteAddress,
        final CapabilityMultiplexer multiplexer,
        final PeerConnectionEventDispatcher peerEventDispatcher,
        final LabelledMetric<Counter> outboundMessagesCounter) {
      super(
          peer,
          peerInfo,
          localAddress,
          remoteAddress,
          multiplexer,
          peerEventDispatcher,
          outboundMessagesCounter);
    }

    static MockPeerConnection create(final Peer peer, final MockP2PNetwork localNetwork) {
      PeerInfo localInfo = localNetwork.ourPeerInfo;
      PeerInfo peerInfo =
          new PeerInfo(
              localInfo.getVersion(),
              localInfo.getClientId(),
              localInfo.getCapabilities(),
              localInfo.getPort(),
              peer.getId());
      InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", localInfo.getPort());
      InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.2", peerInfo.getPort());
      CapabilityMultiplexer multiplexer =
          new CapabilityMultiplexer(
              localNetwork.subProtocols, localInfo.getCapabilities(), peerInfo.getCapabilities());
      PeerConnectionEventDispatcher dispatcher = localNetwork;
      LabelledMetric<Counter> counter = NoOpMetricsSystem.NO_OP_LABELLED_3_COUNTER;
      return new MockPeerConnection(
          peer, peerInfo, localAddress, remoteAddress, multiplexer, dispatcher, counter);
    }

    @Override
    protected void sendOutboundMessage(final OutboundMessage message) {
      // Not implemented
    }

    @Override
    protected void closeConnection(final boolean withDelay) {
      // Nothing to do
    }
  }

  public static class Disconnection {
    private final PeerConnection connection;
    private final DisconnectReason reason;
    private final boolean initiatedByPeer;

    private Disconnection(
        final PeerConnection connection,
        final DisconnectReason reason,
        final boolean initiatedByPeer) {
      this.connection = connection;
      this.reason = reason;
      this.initiatedByPeer = initiatedByPeer;
    }

    public static Disconnection create(
        final PeerConnection connection,
        final DisconnectReason reason,
        final boolean initiatedByPeer) {
      return new Disconnection(connection, reason, initiatedByPeer);
    }

    public PeerConnection getConnection() {
      return connection;
    }

    public DisconnectReason getReason() {
      return reason;
    }

    public boolean isInitiatedByPeer() {
      return initiatedByPeer;
    }
  }
}
