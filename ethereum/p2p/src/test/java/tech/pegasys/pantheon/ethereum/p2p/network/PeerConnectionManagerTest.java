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
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.spy;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//
//import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
//import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.PeerConnectionManager;
//import tech.pegasys.pantheon.ethereum.p2p.rlpx.peers.PeerConnectionManager.PeerConnector;
//import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.TooManyPeersConnectionException;
//import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
//import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
//import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
//import tech.pegasys.pantheon.metrics.MetricsSystem;
//import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
//import tech.pegasys.pantheon.util.enode.EnodeURL;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.OptionalInt;
//import java.util.concurrent.CompletableFuture;
//import java.util.stream.Stream;
//
//import org.junit.Test;
//
//public class PeerConnectionManagerTest {
//
//  final EnodeURL ourEnode = new EnodeURL(Peer.randomId(), "127.0.0.1", 30303, OptionalInt.empty());
//  final List<Peer> availablePeers = new ArrayList<>();
//  final TestPeerConnector peerConnector = spy(new TestPeerConnector());
//  final PeerBlacklist peerBlacklist = new PeerBlacklist();
//  final MetricsSystem metrics = new NoOpMetricsSystem();
//
//  private PeerConnectionManager getPeerConnectionManager(final int maxPeers) {
//    PeerConnectionManager manager =
//        new PeerConnectionManager(
//            peerConnector, () -> availablePeers.stream(), maxPeers, peerBlacklist, metrics);
//    manager.setEnodeUrl(ourEnode);
//    manager.setEnabled();
//    return manager;
//  }
//
//  private PeerConnectionManager getPeerConnectionManager() {
//    return getPeerConnectionManager(25);
//  }
//
//  @Test
//  public void connect_succeeds() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager(5);
//    final Peer peer = P2PNetworkTestHelper.mockPeer();
//    peerConnectionManager.maybeConnect(peer);
//
//    assertThat(peerConnectionManager.getConnections().map(PeerConnection::getPeer)).contains(peer);
//  }
//
//  @Test
//  public void handleIncomingConnection_succeeds() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager(5);
//    PeerConnection connection = P2PNetworkTestHelper.mockPeerConnection();
//    peerConnectionManager.handleIncomingConnection(connection);
//
//    assertThat(peerConnectionManager.getConnections()).contains(connection);
//  }
//
//  @Test
//  public void connect_enforceMaxPeers() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager(1);
//    final Peer peer1 = P2PNetworkTestHelper.mockPeer();
//    peerConnectionManager.maybeConnect(peer1);
//
//    assertThat(peerConnectionManager.getConnections().map(PeerConnection::getPeer)).contains(peer1);
//
//    final Peer peer2 = P2PNetworkTestHelper.mockPeer();
//    CompletableFuture<PeerConnection> result = peerConnectionManager.maybeConnect(peer2);
//    assertThatThrownBy(() -> result.get())
//        .hasCauseInstanceOf(TooManyPeersConnectionException.class);
//
//    assertThat(peerConnectionManager.getConnections().count()).isEqualTo(1);
//  }
//
//  @Test
//  public void handleIncomingConnection_enforceMaxPeers() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager(1);
//    final Peer peer1 = P2PNetworkTestHelper.mockPeer();
//    peerConnectionManager.maybeConnect(peer1);
//
//    assertThat(peerConnectionManager.getConnections().map(PeerConnection::getPeer)).contains(peer1);
//
//    final PeerConnection connection = P2PNetworkTestHelper.mockPeerConnection();
//    peerConnectionManager.handleIncomingConnection(connection);
//
//    // One of the 2 peers should be disconnected
//    assertThat(peerConnectionManager.getConnections().filter(conn -> conn.isDisconnected()).count())
//        .isEqualTo(1);
//  }
//
//  @Test
//  public void addMaintainedPeer_connectsToPeer() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    final Peer peer = P2PNetworkTestHelper.mockPeer();
//    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isTrue();
//
//    assertThat(peerConnectionManager.getMaintainedPeers()).contains(peer);
//    verify(peerConnector, times(1)).connect(peer);
//  }
//
//  @Test
//  public void addMaintainedPeer_repeatedInvocationsReturnFalse() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    final Peer peer = P2PNetworkTestHelper.mockPeer();
//    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isTrue();
//    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isFalse();
//  }
//
//  @Test
//  public void checkMaintainedConnection_connectsNewPeer() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    final Peer peer = P2PNetworkTestHelper.mockPeer();
//
//    peerConnectionManager.maintainedPeers.add(peer);
//    verify(peerConnector, times(0)).connect(peer);
//    assertThat(peerConnectionManager.getConnections().map(PeerConnection::getPeer))
//        .doesNotContain(peer);
//
//    peerConnectionManager.checkMaintainedPeers();
//    verify(peerConnector, times(1)).connect(peer);
//  }
//
//  @Test
//  public void checkMaintainedConnection_doesntReconnectConnectedPeers() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    final Peer peer = P2PNetworkTestHelper.mockPeer();
//
//    // Connect to peer
//    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isTrue();
//    verify(peerConnector, times(1)).connect(peer);
//    assertThat(peerConnectionManager.getConnections().map(PeerConnection::getPeer)).contains(peer);
//
//    // Check shouldn't connect to already connecting peer
//    peerConnectionManager.checkMaintainedPeers();
//    verify(peerConnector, times(1)).connect(peer);
//  }
//
//  @Test
//  public void checkMaintainedConnection_doesntReconnectPendingPeers() {
//    peerConnector.setAutoComplete(false);
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    final Peer peer = P2PNetworkTestHelper.mockPeer();
//
//    // Connect to peer
//    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isTrue();
//    verify(peerConnector, times(1)).connect(peer);
//    assertThat(peerConnectionManager.getConnections().map(PeerConnection::getPeer))
//        .doesNotContain(peer);
//
//    // Check shouldn't connect to peer with pending connection
//    peerConnectionManager.checkMaintainedPeers();
//    verify(peerConnector, times(1)).connect(peer);
//  }
//
//  @Test
//  public void removeMaintainedPeer_returnsFalseIfNotInMaintainedListButDisconnectsPeer() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    final PeerConnection peerConnection = P2PNetworkTestHelper.mockPeerConnection();
//
//    // Connect to the peer
//    peerConnectionManager.handleIncomingConnection(peerConnection);
//    assertThat(peerConnectionManager.getConnections()).contains(peerConnection);
//    assertThat(peerConnection.isDisconnected()).isFalse();
//
//    assertThat(peerConnectionManager.removeMaintainedPeer(peerConnection.getPeer())).isFalse();
//    assertThat(peerConnection.isDisconnected()).isTrue();
//    verify(peerConnection).disconnect(DisconnectReason.REQUESTED);
//  }
//
//  @Test
//  public void attemptPeerConnections_connectsTopeer() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    Peer peer = P2PNetworkTestHelper.mockPeer();
//    availablePeers.add(peer);
//
//    peerConnectionManager.connectToAvailablePeers();
//
//    verify(peerConnector, times(1)).connect(peer);
//  }
//
//  @Test
//  public void attemptPeerConnections_ignoresConnectedPeer() {
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    Peer peer = P2PNetworkTestHelper.mockPeer();
//    availablePeers.add(peer);
//
//    peerConnectionManager.maybeConnect(peer);
//    verify(peerConnector, times(1)).connect(peer);
//
//    // We should not try to connect to already connected peer
//    peerConnectionManager.connectToAvailablePeers();
//    verify(peerConnector, times(1)).connect(peer);
//  }
//
//  @Test
//  public void attemptPeerConnections_ignoresConnectingPeer() {
//    peerConnector.setAutoComplete(false);
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
//    Peer peer = P2PNetworkTestHelper.mockPeer();
//    availablePeers.add(peer);
//
//    peerConnectionManager.maybeConnect(peer);
//    verify(peerConnector, times(1)).connect(peer);
//
//    // We should not try to connect to already connected peer
//    peerConnectionManager.connectToAvailablePeers();
//    verify(peerConnector, times(1)).connect(peer);
//  }
//
//  @Test
//  public void attemptPeerConnections_withSlotsAvailable() {
//    final int maxPeers = 6;
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager(maxPeers);
//    Stream.generate(P2PNetworkTestHelper::mockPeer).limit(10).forEach(availablePeers::add);
//
//    // Sanity check
//    assertThat(peerConnectionManager.getConnections().count()).isEqualTo(0);
//
//    // We should connect to maxPeers
//    peerConnectionManager.connectToAvailablePeers();
//    assertThat(peerConnectionManager.getConnections().count()).isEqualTo(maxPeers);
//  }
//
//  @Test
//  public void attemptPeerConnections_withNoSlotsAvailable() {
//    final int maxPeers = 1;
//    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager(maxPeers);
//    peerConnectionManager.maybeConnect(P2PNetworkTestHelper.mockPeer());
//    Stream.generate(P2PNetworkTestHelper::mockPeer).limit(10).forEach(availablePeers::add);
//
//    // Sanity check
//    assertThat(peerConnectionManager.getConnections().count()).isEqualTo(1);
//
//    // We should connect to maxPeers
//    peerConnectionManager.connectToAvailablePeers();
//    assertThat(peerConnectionManager.getConnections().count()).isEqualTo(1);
//    availablePeers.forEach(
//        (p) -> {
//          verify(peerConnector, never()).connect(p);
//        });
//  }
//
//  private static class TestPeerConnector implements PeerConnector {
//    private boolean shouldAutoCompleteConnection = true;
//
//    @Override
//    public CompletableFuture<PeerConnection> connect(Peer peer) {
//      PeerConnection connection = P2PNetworkTestHelper.mockPeerConnection(peer);
//      final OutgoingConnection conn = new OutgoingConnection(connection);
//
//      if (shouldAutoCompleteConnection) {
//        conn.complete();
//      }
//
//      return conn.getFuture();
//    }
//
//    public void setAutoComplete(boolean shouldAutoCompleteConnection) {
//      this.shouldAutoCompleteConnection = shouldAutoCompleteConnection;
//    }
//  }
//
//  private static class OutgoingConnection {
//    private final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
//    private final PeerConnection peerConnection;
//
//    OutgoingConnection(PeerConnection peerConnection) {
//      this.peerConnection = peerConnection;
//    }
//
//    public void complete() {
//      connectionFuture.complete(peerConnection);
//    }
//
//    public CompletableFuture<PeerConnection> getFuture() {
//      return connectionFuture;
//    }
//  }
//}
