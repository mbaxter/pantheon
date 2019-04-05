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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.VertxPeerDiscoveryAgent;
import tech.pegasys.pantheon.ethereum.p2p.peers.Endpoint;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.SubProtocol;
import tech.pegasys.pantheon.ethereum.permissioning.NodeLocalConfigPermissioningController;
import tech.pegasys.pantheon.ethereum.permissioning.node.NodePermissioningController;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

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

public class NettyP2PNetwork extends AbstractP2PNetwork
    implements P2PNetwork, PeerConnectionEventDispatcher {

  private static final Logger LOG = LogManager.getLogger();
  private static final int TIMEOUT_SECONDS = 30;

  private ChannelFuture server;
  private final EventLoopGroup boss = new NioEventLoopGroup(1);
  private final EventLoopGroup workers = new NioEventLoopGroup(1);

  private final List<SubProtocol> subProtocols;
  private final LabelledMetric<Counter> outboundMessagesCounter;

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a peer networking service for production purposes.
   *
   * <p>The caller is expected to provide the IP address to be advertised (normally this node's
   * public IP address), as well as TCP and UDP port numbers for the RLPx agent and the discovery
   * agent, respectively.
   *
   * @param discoveryAgent The agent responsible for discovery network peers.
   * @param keyPair This node's keypair.
   * @param config The network configuration to use.
   * @param supportedCapabilities The wire protocol capabilities to advertise to connected peers.
   * @param peerBlacklist The peers with which this node will not connect
   * @param metricsSystem The metrics system to capture metrics with.
   * @param nodePermissioningController Controls node permissioning.
   * @param blockchain The blockchain to subscribe to BlockAddedEvents.
   */
  private NettyP2PNetwork(
      final PeerDiscoveryAgent discoveryAgent,
      final SECP256K1.KeyPair keyPair,
      final NetworkingConfiguration config,
      final List<Capability> supportedCapabilities,
      final PeerBlacklist peerBlacklist,
      final MetricsSystem metricsSystem,
      final Optional<NodePermissioningController> nodePermissioningController,
      final Blockchain blockchain) {
    super(
        discoveryAgent,
        keyPair,
        config,
        supportedCapabilities,
        peerBlacklist,
        metricsSystem,
        nodePermissioningController,
        blockchain);

    this.subProtocols = config.getSupportedProtocols();
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

  @Override
  protected CompletableFuture<Void> stopListening() {
    CompletableFuture<Void> stoppedFuture = new CompletableFuture<>();
    workers.shutdownGracefully();
    boss.shutdownGracefully();
    server
        .channel()
        .closeFuture()
        .addListener(
            (future) -> {
              if (future.isSuccess()) {
                stoppedFuture.complete(null);
              } else {
                stoppedFuture.completeExceptionally(future.cause());
              }
            });
    return stoppedFuture;
  }

  @Override
  protected CompletableFuture<Integer> startListening(
      final RlpxConfiguration config, final List<Capability> supportedCapabilities) {
    CompletableFuture<Integer> listeningPortFuture = new CompletableFuture<>();
    this.server =
        new ServerBootstrap()
            .group(boss, workers)
            .channel(NioServerSocketChannel.class)
            .childHandler(inboundChannelInitializer())
            .bind(config.getBindHost(), config.getBindPort());
    server.addListener(
        future -> {
          final InetSocketAddress socketAddress =
              (InetSocketAddress) server.channel().localAddress();
          final String message =
              String.format(
                  "Unable start up P2P network on %s:%s.  Check for port conflicts.",
                  config.getBindHost(), config.getBindPort());

          if (!future.isSuccess()) {
            LOG.error(message, future.cause());
          }
          checkState(socketAddress != null, message);

          LOG.info("P2PNetwork started and listening on {}", socketAddress);
          final int listeningPort = socketAddress.getPort();
          listeningPortFuture.complete(listeningPort);
        });

    return listeningPortFuture;
  }

  @Override
  protected CompletableFuture<PeerConnection> initiateConnection(final Peer peer) {
    final PeerConnectionEventDispatcher peerConnectionEventHandler = this;
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
    final Endpoint endpoint = peer.getEndpoint();

    LOG.trace("Initiating connection to peer: {}", peer.getId());

    new Bootstrap()
        .group(workers)
        .channel(NioSocketChannel.class)
        .remoteAddress(new InetSocketAddress(endpoint.getHost(), endpoint.getFunctionalTcpPort()))
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
                            ourPeerInfo,
                            connectionFuture,
                            peerConnectionEventHandler,
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

    return connectionFuture;
  }

  /** @return a channel initializer for inbound connections */
  private ChannelInitializer<SocketChannel> inboundChannelInitializer() {
    final NettyP2PNetwork self = this;
    final PeerConnectionEventDispatcher peerConnectionEventHandler = this;
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
                    ourPeerInfo,
                    connectionFuture,
                    peerConnectionEventHandler,
                    outboundMessagesCounter));

        connectionFuture.thenAccept(self::handleIncomingConnection);
      }
    };
  }

  private Supplier<Integer> pendingTaskCounter(final EventLoopGroup eventLoopGroup) {
    return () ->
        StreamSupport.stream(eventLoopGroup.spliterator(), false)
            .filter(eventExecutor -> eventExecutor instanceof SingleThreadEventExecutor)
            .mapToInt(eventExecutor -> ((SingleThreadEventExecutor) eventExecutor).pendingTasks())
            .sum();
  }

  public static class Builder {

    protected PeerDiscoveryAgent peerDiscoveryAgent;
    protected KeyPair keyPair;
    protected NetworkingConfiguration config = NetworkingConfiguration.create();
    protected List<Capability> supportedCapabilities;
    protected PeerBlacklist peerBlacklist;
    protected MetricsSystem metricsSystem;
    protected Optional<NodePermissioningController> nodePermissioningController = Optional.empty();
    protected Blockchain blockchain = null;
    private Vertx vertx;
    private Optional<NodeLocalConfigPermissioningController>
        nodeLocalConfigPermissioningController = Optional.empty();

    protected P2PNetwork doBuild() {
      return new NettyP2PNetwork(
          peerDiscoveryAgent,
          keyPair,
          config,
          supportedCapabilities,
          peerBlacklist,
          metricsSystem,
          nodePermissioningController,
          blockchain);
    }

    protected void validate() {
      checkNotNull(keyPair);
      checkNotNull(config);
      checkState(supportedCapabilities != null && supportedCapabilities.size() > 0);
      checkNotNull(peerBlacklist);
      checkNotNull(metricsSystem);
      checkState(!nodePermissioningController.isPresent() || blockchain != null);
      peerDiscoveryAgent = peerDiscoveryAgent == null ? createDiscoveryAgent() : peerDiscoveryAgent;
    }

    protected PeerDiscoveryAgent createDiscoveryAgent() {
      return new VertxPeerDiscoveryAgent(
          vertx,
          keyPair,
          config.getDiscovery(),
          peerBlacklist,
          nodeLocalConfigPermissioningController,
          nodePermissioningController,
          metricsSystem);
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

    public P2PNetwork build() {
      validate();
      this.peerDiscoveryAgent = createDiscoveryAgent();
      return doBuild();
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

    public Builder peerBlacklist(final PeerBlacklist peerBlacklist) {
      this.peerBlacklist = peerBlacklist;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
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
