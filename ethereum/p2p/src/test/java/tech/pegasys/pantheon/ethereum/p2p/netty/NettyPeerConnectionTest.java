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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection.PeerNotConnected;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Endpoint;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.HelloMessage;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Arrays;
import java.util.OptionalInt;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

public class NettyPeerConnectionTest {

  private final ChannelHandlerContext context = mock(ChannelHandlerContext.class);
  private final Channel channel = mock(Channel.class);
  private final ChannelFuture closeFuture = mock(ChannelFuture.class);
  private final EventLoop eventLoop = mock(EventLoop.class);
  private final CapabilityMultiplexer multiplexer = mock(CapabilityMultiplexer.class);
  private final PeerConnectionEventDispatcher peerEventDispatcher =
      mock(PeerConnectionEventDispatcher.class);
  private final BytesValue peerId = Peer.randomId();
  private final int peerPort = 30303;
  private final Peer peer =
      new DefaultPeer(peerId, new Endpoint("127.0.0.1", peerPort, OptionalInt.empty()));
  private final PeerInfo peerInfo =
      new PeerInfo(5, "foo", Arrays.asList(Capability.create("eth", 63)), peerPort, peerId);

  private NettyPeerConnection connection;

  @Before
  public void setUp() {
    when(context.channel()).thenReturn(channel);
    when(channel.closeFuture()).thenReturn(closeFuture);
    when(channel.eventLoop()).thenReturn(eventLoop);
    connection =
        new NettyPeerConnection(
            context,
            peer,
            peerInfo,
            multiplexer,
            peerEventDispatcher,
            NoOpMetricsSystem.NO_OP_LABELLED_COUNTER);
  }

  @Test
  public void shouldThrowExceptionWhenAttemptingToSendMessageOnClosedConnection() {
    connection.disconnect(DisconnectReason.SUBPROTOCOL_TRIGGERED);
    Assertions.assertThatThrownBy(() -> connection.send(null, HelloMessage.create(peerInfo)))
        .isInstanceOfAny(PeerNotConnected.class);
  }
}
