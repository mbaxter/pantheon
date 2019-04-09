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
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
//import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.AbstractPeerConnection;
//import tech.pegasys.pantheon.ethereum.p2p.wire.CapabilityMultiplexer;
//import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.netty.PeerConnectionEventDispatcher;
//import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.netty.OutboundMessage;
//import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
//import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
//import tech.pegasys.pantheon.metrics.Counter;
//import tech.pegasys.pantheon.metrics.LabelledMetric;
//import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
//
//import java.net.InetSocketAddress;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.List;
//
//class MockPeerConnection extends AbstractPeerConnection {
//
//  MockPeerConnection(
//      final Peer peer,
//      final PeerInfo peerInfo,
//      final InetSocketAddress localAddress,
//      final InetSocketAddress remoteAddress,
//      final CapabilityMultiplexer multiplexer,
//      final PeerConnectionEventDispatcher peerEventDispatcher,
//      final LabelledMetric<Counter> outboundMessagesCounter) {
//    super(
//        peer,
//        peerInfo,
//        localAddress,
//        remoteAddress,
//        multiplexer,
//        peerEventDispatcher,
//        outboundMessagesCounter);
//  }
//
//  static MockPeerConnection create(final Peer peer) {
//    final List<Capability> capabilities = Arrays.asList(Capability.create("eth", 63));
//    PeerInfo peerInfo =
//        new PeerInfo(5, "test", capabilities, peer.getEnodeURL().getListeningPort(), peer.getId());
//    InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", 30303);
//    InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.2", peerInfo.getPort());
//    CapabilityMultiplexer multiplexer = mock(CapabilityMultiplexer.class);
//    when(multiplexer.getAgreedCapabilities()).thenReturn(Collections.emptySet());
//    PeerConnectionEventDispatcher dispatcher = PeerConnectionEventDispatcher.NOOP;
//    LabelledMetric<Counter> counter = NoOpMetricsSystem.NO_OP_LABELLED_3_COUNTER;
//    return new MockPeerConnection(
//        peer, peerInfo, localAddress, remoteAddress, multiplexer, dispatcher, counter);
//  }
//
//  static MockPeerConnection create(final Peer peer, final MockP2PNetwork localNetwork) {
//    final PeerInfo localInfo = localNetwork.ourPeerInfo;
//    final PeerInfo peerInfo =
//        new PeerInfo(
//            localInfo.getVersion(),
//            localInfo.getClientId(),
//            localInfo.getCapabilities(),
//            peer.getEnodeURL().getListeningPort(),
//            peer.getId());
//    final InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", localInfo.getPort());
//    final InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.2", peerInfo.getPort());
//
//    CapabilityMultiplexer multiplexer = mock(CapabilityMultiplexer.class);
//    when(multiplexer.getAgreedCapabilities())
//        .thenReturn(new HashSet<>(localNetwork.ourCapabilities));
//    final PeerConnectionEventDispatcher dispatcher = localNetwork;
//    final LabelledMetric<Counter> counter = NoOpMetricsSystem.NO_OP_LABELLED_3_COUNTER;
//    return new MockPeerConnection(
//        peer, peerInfo, localAddress, remoteAddress, multiplexer, dispatcher, counter);
//  }
//
//  @Override
//  protected void sendOutboundMessage(final OutboundMessage message) {
//    // Not implemented
//  }
//
//  @Override
//  protected void closeConnection(final boolean withDelay) {
//    // Nothing to do
//  }
//}
