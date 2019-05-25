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
package tech.pegasys.pantheon.ethereum.p2p.rlpx.netty;

import static com.google.common.base.Preconditions.checkState;

import java.util.function.IntSupplier;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.LocalNode;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.RlpxAgent;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.PeerConnectionEventDispatcher;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NettyRlpxAgent extends RlpxAgent {
  private static final Logger LOG = LogManager.getLogger();
  private static final int TIMEOUT_SECONDS = 30;

  private ChannelFuture server;
  private final EventLoopGroup boss = new NioEventLoopGroup(1);
  private final EventLoopGroup workers = new NioEventLoopGroup(1);

  private final Collection<PeerConnection> connections = new ConcurrentLinkedQueue<>();
  private final PeerConnectionEventDispatcher eventDispatcher;

  private final LabelledMetric<Counter> outboundMessagesCounter;

  public NettyRlpxAgent(
      final KeyPair keyPair,
      final RlpxConfiguration config,
      final LocalNode localNode,
      final MetricsSystem metricsSystem) {
    super(keyPair, config, localNode, metricsSystem);
    this.eventDispatcher =
        new PeerConnectionEventDispatcher(this::dispatchDisconnect, this::dispatchMessage);

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
  protected CompletableFuture<Integer> startListening() {
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
  public Stream<? extends PeerConnection> streamConnections() {
    return connections.stream();
  }

  @Override
  public int getConnectionCount() {
    return connections.size();
  }

  @Override
  protected CompletableFuture<PeerConnection> doConnect(final Peer peer) {
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();

    if (peer instanceof DiscoveryPeer) {
      ((DiscoveryPeer) peer).setLastAttemptedConnection(System.currentTimeMillis());
    }

    final EnodeURL enode = peer.getEnodeURL();
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
                            config.getSupportedProtocols(),
                            localNode,
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

    return connectionFuture;
  }

  /** @return a channel initializer for inbound connections */
  private ChannelInitializer<SocketChannel> inboundChannelInitializer() {
    final NettyRlpxAgent self = this;
    return new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(final SocketChannel ch) {
        final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
        connectionFuture.thenAccept(
            connection -> {
              LOG.debug("Inbound connection established with {}", connection.getPeer().getId());
              self.dispatchConnect(connection);
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
                    config.getSupportedProtocols(),
                    localNode,
                    connectionFuture,
                    eventDispatcher,
                    outboundMessagesCounter));
      }
    };
  }

  @Override
  protected void dispatchDisconnect(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    connections.remove(connection);
    super.dispatchDisconnect(connection, reason, initiatedByPeer);
  }

  @Override
  protected void dispatchConnect(final PeerConnection connection) {
    connections.add(connection);
    if (connection.isDisconnected()) {
      // Nothing to dispatch
      connections.remove(connection);
      return;
    }
    super.dispatchConnect(connection);
  }

  private IntSupplier pendingTaskCounter(final EventLoopGroup eventLoopGroup) {
    return () ->
        StreamSupport.stream(eventLoopGroup.spliterator(), false)
            .filter(eventExecutor -> eventExecutor instanceof SingleThreadEventExecutor)
            .mapToInt(eventExecutor -> ((SingleThreadEventExecutor) eventExecutor).pendingTasks())
            .sum();
  }
}
