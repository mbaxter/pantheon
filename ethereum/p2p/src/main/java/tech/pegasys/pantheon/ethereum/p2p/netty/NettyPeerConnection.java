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

import static java.util.concurrent.TimeUnit.SECONDS;
import static tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason.TCP_SUBSYSTEM_ERROR;

import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

final class NettyPeerConnection extends AbstractPeerConnection implements PeerConnection {

  private final ChannelHandlerContext ctx;

  public NettyPeerConnection(
      final ChannelHandlerContext ctx,
      final Peer peer,
      final PeerInfo peerInfo,
      final CapabilityMultiplexer multiplexer,
      final PeerConnectionEventDispatcher peerEventDispatcher,
      final LabelledMetric<Counter> outboundMessagesCounter) {
    super(
        peer,
        peerInfo,
        localAddress(ctx),
        remoteAddress(ctx),
        multiplexer,
        peerEventDispatcher,
        outboundMessagesCounter);
    this.ctx = ctx;
    ctx.channel().closeFuture().addListener(f -> terminateConnection(TCP_SUBSYSTEM_ERROR, false));
  }

  private static InetSocketAddress remoteAddress(final ChannelHandlerContext ctx) {
    return (InetSocketAddress) ctx.channel().localAddress();
  }

  private static InetSocketAddress localAddress(final ChannelHandlerContext ctx) {
    return (InetSocketAddress) ctx.channel().localAddress();
  }

  @Override
  protected void sendOutboundMessage(final OutboundMessage message) {
    ctx.channel().writeAndFlush(message);
  }

  @Override
  protected void closeConnection(final boolean withDelay) {
    if (withDelay) {
      ctx.channel().eventLoop().schedule((Callable<ChannelFuture>) ctx::close, 2L, SECONDS);
    } else {
      ctx.close();
    }
  }
}
