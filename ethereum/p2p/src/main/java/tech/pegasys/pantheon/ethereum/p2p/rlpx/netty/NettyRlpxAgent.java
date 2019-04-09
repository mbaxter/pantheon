package tech.pegasys.pantheon.ethereum.p2p.rlpx.netty;

import static com.google.common.base.Preconditions.checkState;

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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.peers.Endpoint;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.RlpxAgent;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.PeerConnectionEventDispatcher;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.SubProtocol;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;

public class NettyRlpxAgent extends RlpxAgent{
  private static final Logger LOG = LogManager.getLogger();
  private static final int TIMEOUT_SECONDS = 30;

  private ChannelFuture server;
  private final EventLoopGroup boss = new NioEventLoopGroup(1);
  private final EventLoopGroup workers = new NioEventLoopGroup(1);

  private final List<PeerConnection> connections = Collections.synchronizedList(new ArrayList<>());
  private final PeerConnectionEventDispatcher eventDispatcher;

  private final KeyPair keyPair;
  private final List<SubProtocol> subProtocols;

  private final LabelledMetric<Counter> outboundMessagesCounter;

  public NettyRlpxAgent(
    final NetworkingConfiguration config,
    final List<Capability> capabilities,
    final List<SubProtocol> subProtocols,
    final KeyPair keyPair,
    final MetricsSystem metricsSystem) {
    super(keyPair.getPublicKey().getEncodedBytes(), config, capabilities, metricsSystem);
    this.eventDispatcher = new PeerConnectionEventDispatcher(this::dispatchDisconnect, this::dispatchMessage);
    this.subProtocols = subProtocols;
    this.keyPair = keyPair;

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

        LOG.info("P2P network started and listening on {}", socketAddress);
        final int listeningPort = socketAddress.getPort();
        listeningPortFuture.complete(listeningPort);
      });

    return listeningPortFuture;
  }

  @Override
  public CompletableFuture<Void> stopListening() {
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
  public Stream<? extends PeerConnection> getConnections() {
    return connections.stream();
  }

  @Override
  protected CompletableFuture<PeerConnection> doConnect(final Peer peer, final CompletableFuture<PeerConnection> connectionFuture) {
    final Endpoint endpoint = peer.getEndpoint();

    LOG.trace("Initiating connection to peer: {}", peer.getId());

    Supplier<PeerInfo> peerInfo = this::getPeerInfo;
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
                  peerInfo,
                  connectionFuture,
                  eventDispatcher,
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

    connectionFuture.whenComplete((conn, err) -> {
      if (err != null) {
        LOG.debug("Failed to connect to peer {}: {}", peer.getId(), err);
      } else {
        LOG.debug("Outbound connection established to peer: {}", peer.getId());
        dispatchConnect(conn, false);
      }});

    return connectionFuture;
  }

  /** @return a channel initializer for inbound connections */
  private ChannelInitializer<SocketChannel> inboundChannelInitializer() {
    final NettyRlpxAgent self = this;
    Supplier<PeerInfo> peerInfo = this::getPeerInfo;
    return new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(final SocketChannel ch) {
        final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
        connectionFuture.thenAccept(connection -> {
          LOG.debug("Inbound connection established with {}", connection.getPeer().getId());
          self.dispatchConnect(connection, true);
        });

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
              peerInfo,
              connectionFuture,
              eventDispatcher,
              outboundMessagesCounter));
      }
    };
  }

  @Override
  protected void dispatchDisconnect(final PeerConnection connection, final DisconnectReason reason,
    final boolean initiatedByPeer) {
    connections.remove(connection);
    super.dispatchDisconnect(connection, reason, initiatedByPeer);
  }

  @Override
  protected void dispatchConnect(final PeerConnection connection, final boolean initiatedByPeer) {
    connections.add(connection);
    if (connection.isDisconnected()) {
      // Nothing to dispatch
      connections.remove(connection);
      return;
    }
    super.dispatchConnect(connection, initiatedByPeer);
  }

  private Supplier<Integer> pendingTaskCounter(final EventLoopGroup eventLoopGroup) {
    return () ->
      StreamSupport.stream(eventLoopGroup.spliterator(), false)
        .filter(eventExecutor -> eventExecutor instanceof SingleThreadEventExecutor)
        .mapToInt(eventExecutor -> ((SingleThreadEventExecutor) eventExecutor).pendingTasks())
        .sum();
  }
}
