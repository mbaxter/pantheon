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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.netty.PeerConnectionManager.PeerConnector;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerTestHelper;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class PeerConnectionManagerTest {

  final List<DiscoveryPeer> discoveryPeers = new ArrayList<>();
  final PeerConnector peerConnector = mock(PeerConnector.class);
  final PeerBlacklist peerBlacklist = new PeerBlacklist();
  final MetricsSystem metrics = new NoOpMetricsSystem();

  private PeerConnectionManager getPeerConnectionManager(final int maxPeers) {
    return new PeerConnectionManager(
        peerConnector, () -> discoveryPeers.stream(), maxPeers, peerBlacklist, metrics);
  }

  private PeerConnectionManager getPeerConnectionManager() {
    return getPeerConnectionManager(25);
  }

  @Test
  public void addingMaintainedNetworkPeerStartsConnection() {
    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
    final Peer peer = PeerTestHelper.mockPeer();
    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isTrue();

    assertThat(peerConnectionManager.getMaintainedPeers()).contains(peer);
    verify(peerConnector, times(1)).connect(peer);
  }

  @Test
  public void addingRepeatMaintainedPeersReturnsFalse() {
    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
    final Peer peer = PeerTestHelper.mockPeer();
    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isTrue();
    assertThat(peerConnectionManager.addMaintainedPeer(peer)).isFalse();
  }

  // TODO: Finish migrating tests
  //  @Test
  //  public void checkMaintainedConnectionPeersTriesToConnect() {
  //    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
  //    final Peer peer = PeerTestHelper.mockPeer();
  //    peerConnectionManager.addMaintainedPeer(peer);
  //
  //    network.checkMaintainedConnectionPeers();
  //    verify(network, times(1)).connect(peer);
  //  }
  //
  //  @Test
  //  public void checkMaintainedConnectionPeersDoesntReconnectPendingPeers() {
  //    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
  //    final Peer peer = PeerTestHelper.mockPeer();
  //
  //    network.pendingConnections.put(peer, new CompletableFuture<>());
  //
  //    network.checkMaintainedConnectionPeers();
  //    verify(network, times(0)).connect(peer);
  //  }
  //
  //  @Test
  //  public void checkMaintainedConnectionPeersDoesntReconnectConnectedPeers() {
  //    final PeerConnectionManager peerConnectionManager = getPeerConnectionManager();
  //    final Peer peer = PeerTestHelper.mockPeer();
  //    verify(network, never()).connect(peer);
  //    assertThat(network.addMaintainConnectionPeer(peer)).isTrue();
  //    verify(network, times(1)).connect(peer);
  //
  //    {
  //      final CompletableFuture<PeerConnection> connection;
  //      connection = network.pendingConnections.remove(peer);
  //      assertThat(connection).isNotNull();
  //      assertThat(connection.cancel(true)).isTrue();
  //    }
  //
  //    {
  //      final PeerConnection peerConnection = mockPeerConnection(peer.getId());
  //      network.connections.registerConnection(peerConnection);
  //      network.checkMaintainedConnectionPeers();
  //      verify(network, times(1)).connect(peer);
  //    }
  //  }
  //
  //  @Test
  //  public void removePeerReturnsTrueIfNodeWasInMaintaineConnectionsAndDisconnectsIfInPending() {
  //    final NettyP2PNetwork nettyP2PNetwork = nettyP2PNetwork();
  //    nettyP2PNetwork.start();
  //
  //    final Peer localPeer = mockPeer("127.0.0.1", 30301);
  //    final Peer remotePeer = mockPeer("127.0.0.2", 30302);
  //    final PeerConnection peerConnection = mockPeerConnection(localPeer, remotePeer);
  //
  //    nettyP2PNetwork.addMaintainConnectionPeer(remotePeer);
  //    assertThat(nettyP2PNetwork.peerMaintainConnectionList.contains(remotePeer)).isTrue();
  //    assertThat(nettyP2PNetwork.pendingConnections.containsKey(remotePeer)).isTrue();
  //    assertThat(nettyP2PNetwork.removeMaintainedConnectionPeer(remotePeer)).isTrue();
  //    assertThat(nettyP2PNetwork.peerMaintainConnectionList.contains(remotePeer)).isFalse();
  //
  //    // Note: The pendingConnection future is not removed.
  //    assertThat(nettyP2PNetwork.pendingConnections.containsKey(remotePeer)).isTrue();
  //
  //    // Complete the connection, and ensure "disconnect is automatically called.
  //    nettyP2PNetwork.pendingConnections.get(remotePeer).complete(peerConnection);
  //    verify(peerConnection).disconnect(DisconnectReason.REQUESTED);
  //  }
  //
  //  @Test
  //  public void removePeerReturnsFalseIfNotInMaintainedListButDisconnectsPeer() {
  //    final NettyP2PNetwork nettyP2PNetwork = nettyP2PNetwork();
  //    nettyP2PNetwork.start();
  //
  //    final Peer localPeer = mockPeer("127.0.0.1", 30301);
  //    final Peer remotePeer = mockPeer("127.0.0.2", 30302);
  //    final PeerConnection peerConnection = mockPeerConnection(localPeer, remotePeer);
  //
  //    CompletableFuture<PeerConnection> future = nettyP2PNetwork.connect(remotePeer);
  //
  //    assertThat(nettyP2PNetwork.peerMaintainConnectionList.contains(remotePeer)).isFalse();
  //    assertThat(nettyP2PNetwork.pendingConnections.containsKey(remotePeer)).isTrue();
  //    future.complete(peerConnection);
  //    assertThat(nettyP2PNetwork.pendingConnections.containsKey(remotePeer)).isFalse();
  //
  //    assertThat(nettyP2PNetwork.removeMaintainedConnectionPeer(remotePeer)).isFalse();
  //    assertThat(nettyP2PNetwork.peerMaintainConnectionList.contains(remotePeer)).isFalse();
  //
  //    verify(peerConnection).disconnect(DisconnectReason.REQUESTED);
  //  }
  //
  //  @Test
  //  public void attemptPeerConnections_connectsToValidPeer() {
  //    final int maxPeers = 5;
  //    final NettyP2PNetwork network =
  //      mockNettyP2PNetwork(() -> RlpxConfiguration.create().setMaxPeers(maxPeers));
  //
  //    doReturn(2).when(network).connectionCount();
  //    DiscoveryPeer peer = createDiscoveryPeer(0);
  //    peer.setStatus(PeerDiscoveryStatus.BONDED);
  //
  //    doReturn(Stream.of(peer)).when(network).getDiscoveryPeers();
  //    ArgumentCaptor<DiscoveryPeer> peerCapture = ArgumentCaptor.forClass(DiscoveryPeer.class);
  //    doReturn(CompletableFuture.completedFuture(mock(PeerConnection.class)))
  //      .when(network)
  //      .connect(peerCapture.capture());
  //
  //    network.connectToAvailablePeers();
  //    verify(network, times(1)).connect(any());
  //    assertThat(peerCapture.getValue()).isEqualTo(peer);
  //  }
  //
  //  @Test
  //  public void attemptPeerConnections_ignoresUnbondedPeer() {
  //    final int maxPeers = 5;
  //    final NettyP2PNetwork network =
  //      mockNettyP2PNetwork(() -> RlpxConfiguration.create().setMaxPeers(maxPeers));
  //
  //    doReturn(2).when(network).connectionCount();
  //    DiscoveryPeer peer = createDiscoveryPeer(0);
  //    peer.setStatus(PeerDiscoveryStatus.KNOWN);
  //
  //    doReturn(Stream.of(peer)).when(network).getDiscoveryPeers();
  //
  //    network.connectToAvailablePeers();
  //    verify(network, times(0)).connect(any());
  //  }
  //
  //  @Test
  //  public void attemptPeerConnections_ignoresConnectingPeer() {
  //    final int maxPeers = 5;
  //    final NettyP2PNetwork network =
  //      mockNettyP2PNetwork(() -> RlpxConfiguration.create().setMaxPeers(maxPeers));
  //
  //    doReturn(2).when(network).connectionCount();
  //    DiscoveryPeer peer = createDiscoveryPeer(0);
  //    peer.setStatus(PeerDiscoveryStatus.BONDED);
  //
  //    doReturn(true).when(network).isConnecting(peer);
  //    doReturn(Stream.of(peer)).when(network).getDiscoveryPeers();
  //
  //    network.connectToAvailablePeers();
  //    verify(network, times(0)).connect(any());
  //  }
  //
  //  @Test
  //  public void attemptPeerConnections_ignoresConnectedPeer() {
  //    final int maxPeers = 5;
  //    final NettyP2PNetwork network =
  //      mockNettyP2PNetwork(() -> RlpxConfiguration.create().setMaxPeers(maxPeers));
  //
  //    doReturn(2).when(network).connectionCount();
  //    DiscoveryPeer peer = createDiscoveryPeer(0);
  //    peer.setStatus(PeerDiscoveryStatus.BONDED);
  //
  //    doReturn(true).when(network).isConnected(peer);
  //    doReturn(Stream.of(peer)).when(network).getDiscoveryPeers();
  //
  //    network.connectToAvailablePeers();
  //    verify(network, times(0)).connect(any());
  //  }
  //
  //  @Test
  //  public void attemptPeerConnections_withSlotsAvailable() {
  //    final int maxPeers = 5;
  //    final NettyP2PNetwork network =
  //      mockNettyP2PNetwork(() -> RlpxConfiguration.create().setMaxPeers(maxPeers));
  //
  //    doReturn(2).when(network).connectionCount();
  //    List<DiscoveryPeer> peers =
  //      Stream.iterate(1, n -> n + 1)
  //        .limit(10)
  //        .map(
  //          (seed) -> {
  //            DiscoveryPeer peer = createDiscoveryPeer(seed);
  //            peer.setStatus(PeerDiscoveryStatus.BONDED);
  //            return peer;
  //          })
  //        .collect(Collectors.toList());
  //
  //    doReturn(peers.stream()).when(network).getDiscoveryPeers();
  //    ArgumentCaptor<DiscoveryPeer> peerCapture = ArgumentCaptor.forClass(DiscoveryPeer.class);
  //    doReturn(CompletableFuture.completedFuture(mock(PeerConnection.class)))
  //      .when(network)
  //      .connect(peerCapture.capture());
  //
  //    network.connectToAvailablePeers();
  //    verify(network, times(3)).connect(any());
  //    assertThat(peers.containsAll(peerCapture.getAllValues())).isTrue();
  //  }
  //
  //  @Test
  //  public void attemptPeerConnections_withNoSlotsAvailable() {
  //    final int maxPeers = 5;
  //    final NettyP2PNetwork network =
  //      mockNettyP2PNetwork(() -> RlpxConfiguration.create().setMaxPeers(maxPeers));
  //
  //    doReturn(maxPeers).when(network).connectionCount();
  //    List<DiscoveryPeer> peers =
  //      Stream.iterate(1, n -> n + 1)
  //        .limit(10)
  //        .map(
  //          (seed) -> {
  //            DiscoveryPeer peer = createDiscoveryPeer(seed);
  //            peer.setStatus(PeerDiscoveryStatus.BONDED);
  //            return peer;
  //          })
  //        .collect(Collectors.toList());
  //
  //    lenient().doReturn(peers.stream()).when(network).getDiscoveryPeers();
  //
  //    network.connectToAvailablePeers();
  //    verify(network, times(0)).connect(any());
  //  }
  //  private DiscoveryPeer createDiscoveryPeer(final int seed) {
  //    return new DiscoveryPeer(generatePeerId(seed), "127.0.0.1", 999, OptionalInt.empty());
  //  }

}
