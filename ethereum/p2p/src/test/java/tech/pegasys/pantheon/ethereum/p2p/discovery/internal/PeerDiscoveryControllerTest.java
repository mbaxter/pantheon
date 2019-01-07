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
package tech.pegasys.pantheon.ethereum.p2p.discovery.internal;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryStatus;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryTestHelper;
import tech.pegasys.pantheon.ethereum.p2p.peers.Endpoint;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.permissioning.NodeWhitelistController;
import tech.pegasys.pantheon.ethereum.permissioning.PermissioningConfiguration;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.bytes.MutableBytesValue;
import tech.pegasys.pantheon.util.uint.UInt256;
import tech.pegasys.pantheon.util.uint.UInt256Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PeerDiscoveryControllerTest {

  private static final byte MOST_SIGNFICANT_BIT_MASK = -128;
  private static final RetryDelayFunction LONG_DELAY_FUNCTION = (prev) -> 999999999L;
  private static final RetryDelayFunction SHORT_DELAY_FUNCTION = (prev) -> Math.max(100, prev * 2);
  private static final PeerRequirement PEER_REQUIREMENT = () -> true;
  private static final long TABLE_REFRESH_INTERVAL_MS = TimeUnit.HOURS.toMillis(1);
  private PeerDiscoveryController controller;
  private DiscoveryPeer self;
  private PeerTable peerTable;
  private NodeWhitelistController defaultNodeWhitelistController;
  private KeyPair selfKeyPair;
  private final AtomicInteger counter = new AtomicInteger(1);

  @Before
  public void initializeMocks() {
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    selfKeyPair = keyPairs[0];
    self = PeerDiscoveryTestHelper.generatePeers(keyPairs)[0];
    peerTable = new PeerTable(self.getId());
  }

  @After
  public void stopTable() {
    if (controller != null) {
      controller.stop().join();
    }
  }

  @Test
  public void bootstrapPeersRetriesSent() {
    // Create peers.
    int peerCount = 3;
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(peerCount);
    final DiscoveryPeer[] peers = PeerDiscoveryTestHelper.generateDiscoveryPeers(keyPairs);

    MockTimerUtil timer = spy(new MockTimerUtil());
    controller = getControllerBuilder().setPeers(peers).setTimerUtil(timer).build();
    controller.setRetryDelayFunction(SHORT_DELAY_FUNCTION);

    // Mock the creation of the PING packet, so that we can control the hash,
    // which gets validated when receiving the PONG.
    final PingPacketData mockPing =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet mockPacket = Packet.create(PacketType.PING, mockPing, keyPairs[0]);
    doReturn(mockPacket).when(controller).createPacket(eq(PacketType.PING), any());

    controller.start();

    int timeouts = 4;
    for (int i = 0; i < timeouts; i++) {
      timer.runTimerHandlers();
    }
    int expectedTimerEvents = (timeouts + 1) * peerCount;
    verify(timer, times(expectedTimerEvents)).setTimer(anyLong(), any());

    // Within this time period, 4 timers should be placed with these timeouts.
    final long[] expectedTimeouts = {100, 200, 400, 800};
    for (final long timeout : expectedTimeouts) {
      verify(timer, times(peerCount)).setTimer(eq(timeout), any());
    }

    // Check that 5 PING packets were sent for each peer (the initial + 4 attempts following
    // timeouts).
    Stream.of(peers)
        .forEach(
            p ->
                verify(controller, times(timeouts + 1))
                    .sendPacket(eq(p), eq(PacketType.PING), any()));

    controller
        .getPeers()
        .forEach(p -> assertThat(p.getStatus()).isEqualTo(PeerDiscoveryStatus.BONDING));
  }

  @Test
  public void bootstrapPeersRetriesStoppedUponResponse() {
    // Create peers.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(3);
    final DiscoveryPeer[] peers = PeerDiscoveryTestHelper.generateDiscoveryPeers(keyPairs);

    MockTimerUtil timer = new MockTimerUtil();
    controller = getControllerBuilder().setPeers(peers).setTimerUtil(timer).build();

    // Mock the creation of the PING packet, so that we can control the hash,
    // which gets validated when receiving the PONG.
    final PingPacketData mockPing =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet mockPacket = Packet.create(PacketType.PING, mockPing, keyPairs[0]);
    doReturn(mockPacket).when(controller).createPacket(eq(PacketType.PING), any());

    controller.start();

    // Invoke timers several times so that ping to peers should be resent
    for (int i = 0; i < 3; i++) {
      timer.runTimerHandlers();
    }

    // Assert PING packet was sent for peer[0] 4 times.
    for (DiscoveryPeer peer : peers) {
      verify(controller, times(4)).sendPacket(eq(peer), eq(PacketType.PING), any());
    }

    // Simulate a PONG message from peer 0.
    final PongPacketData packetData =
        PongPacketData.create(self.getEndpoint(), mockPacket.getHash());
    final Packet packet = Packet.create(PacketType.PONG, packetData, keyPairs[0]);
    controller.onMessage(packet, peers[0]);

    // Invoke timers again
    for (int i = 0; i < 4; i++) {
      timer.runTimerHandlers();
    }

    // Ensure we receive no more PING packets for peer[0].
    // Assert PING packet was sent for peer[0] 4 times.
    for (DiscoveryPeer peer : peers) {
      int expectedCount = peer.equals(peers[0]) ? 4 : 8;
      verify(controller, times(expectedCount)).sendPacket(eq(peer), eq(PacketType.PING), any());
    }
  }

  @Test
  public void bootstrapPeersPongReceived_HashMatched() {
    // Create peers.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(3);
    final DiscoveryPeer[] peers = PeerDiscoveryTestHelper.generateDiscoveryPeers(keyPairs);

    MockTimerUtil timer = new MockTimerUtil();
    controller = getControllerBuilder().setPeers(peers).setTimerUtil(timer).build();

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when receiving the PONG.
    final PingPacketData mockPing =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet mockPacket = Packet.create(PacketType.PING, mockPing, keyPairs[0]);
    doReturn(mockPacket).when(controller).createPacket(eq(PacketType.PING), any());

    controller.start();

    assertThat(
            controller
                .getPeers()
                .stream()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(3);

    // Simulate a PONG message from peer 0.
    final PongPacketData packetData =
        PongPacketData.create(self.getEndpoint(), mockPacket.getHash());
    final Packet packet = Packet.create(PacketType.PONG, packetData, keyPairs[0]);
    controller.onMessage(packet, peers[0]);

    // Ensure that the peer controller is now sending FIND_NEIGHBORS messages for this peer.
    verify(controller, times(1)).sendPacket(eq(peers[0]), eq(PacketType.FIND_NEIGHBORS), any());
    // Invoke timeouts and check that we resent our neighbors request
    timer.runTimerHandlers();
    verify(controller, times(2)).sendPacket(eq(peers[0]), eq(PacketType.FIND_NEIGHBORS), any());

    assertThat(
            controller
                .getPeers()
                .stream()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(2);
    assertThat(
            controller.getPeers().stream().filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDED))
        .hasSize(1);
  }

  @Test
  public void bootstrapPeersPongReceived_HashUnmatched() {
    // Create peers.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(3);
    final DiscoveryPeer[] peers = PeerDiscoveryTestHelper.generateDiscoveryPeers(keyPairs);

    controller = createController(Arrays.asList(peers));
    controller.setRetryDelayFunction(LONG_DELAY_FUNCTION);

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when
    // processing the PONG.
    final PingPacketData mockPing =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet mockPacket = Packet.create(PacketType.PING, mockPing, keyPairs[0]);
    doReturn(mockPacket).when(controller).createPacket(eq(PacketType.PING), any());

    controller.start();

    assertThat(
            controller
                .getPeers()
                .stream()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(3);

    // Send a PONG packet from peer 1, with an incorrect hash.
    final PongPacketData packetData =
        PongPacketData.create(self.getEndpoint(), BytesValue.fromHexString("1212"));
    final Packet packet = Packet.create(PacketType.PONG, packetData, keyPairs[1]);
    controller.onMessage(packet, peers[1]);

    // No FIND_NEIGHBORS packet was sent for peer 1.
    verify(controller, never()).sendPacket(eq(peers[1]), eq(PacketType.FIND_NEIGHBORS), any());

    assertThat(
            controller
                .getPeers()
                .stream()
                .filter(p -> p.getStatus() == PeerDiscoveryStatus.BONDING))
        .hasSize(3);
  }

  @Test
  public void findNeighborsSentAfterBondingFinished() {
    // Create three peers, out of which the first two are bootstrap peers.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    final DiscoveryPeer[] peers = PeerDiscoveryTestHelper.generateDiscoveryPeers(keyPairs);
    final OutboundMessageHandler messageHandler = mock(OutboundMessageHandler.class);

    // Initialize the peer controller, setting a high controller refresh interval and a high timeout
    // threshold,
    // to avoid retries getting in the way of this test.
    controller = createController(Collections.singletonList(peers[0]));

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when
    // processing the PONG.
    final PingPacketData mockPing =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet mockPacket = Packet.create(PacketType.PING, mockPing, keyPairs[0]);
    doReturn(mockPacket).when(controller).createPacket(eq(PacketType.PING), any());
    controller.setRetryDelayFunction((prev) -> 999999999L);
    controller.start();

    // Verify that the PING was sent.
    verify(controller, times(1)).sendPacket(eq(peers[0]), eq(PacketType.PING), any());

    // Simulate a PONG message from peer[0].
    final PongPacketData packetData =
        PongPacketData.create(self.getEndpoint(), mockPacket.getHash());
    final Packet pongPacket = Packet.create(PacketType.PONG, packetData, keyPairs[0]);
    controller.onMessage(pongPacket, peers[0]);

    // Verify that the FIND_NEIGHBORS packet was sent with target == self.
    final ArgumentCaptor<PacketData> captor = ArgumentCaptor.forClass(PacketData.class);
    verify(controller, times(1))
        .sendPacket(eq(peers[0]), eq(PacketType.FIND_NEIGHBORS), captor.capture());

    assertThat(captor.getValue()).isInstanceOf(FindNeighborsPacketData.class);
    final FindNeighborsPacketData data = (FindNeighborsPacketData) captor.getValue();
    assertThat(data.getTarget()).isEqualTo(self.getId());
    assertThat(controller.getPeers()).hasSize(1);
    assertThat(controller.getPeers().stream().findFirst().get().getStatus())
        .isEqualTo(PeerDiscoveryStatus.BONDED);
  }

  private PeerDiscoveryController createController(Collection<DiscoveryPeer> discoPeers) {
    return getControllerBuilder().setPeers(discoPeers).build();
  }

  private PeerDiscoveryController createController(DiscoveryPeer... discoPeers) {
    return getControllerBuilder().setPeers(discoPeers).build();
  }

  private PeerDiscoveryController createController(
      Collection<DiscoveryPeer> discoPeers, PeerBlacklist blacklist) {
    return getControllerBuilder().setPeers(discoPeers).setBlacklist(blacklist).build();
  }

  private PeerDiscoveryController createController(
      Collection<DiscoveryPeer> discoPeers,
      PeerBlacklist blacklist,
      NodeWhitelistController whitelist) {
    return getControllerBuilder()
        .setPeers(discoPeers)
        .setBlacklist(blacklist)
        .setWhitelist(whitelist)
        .build();
  }

  private ControllerBuilder getControllerBuilder() {
    return ControllerBuilder.create().setKeyPair(selfKeyPair).setSelf(self).setPeerTable(peerTable);
  }

  @Test
  public void peerSeenTwice() throws InterruptedException {
    // Create three peers, out of which the first two are bootstrap peers.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(3);
    final DiscoveryPeer[] peers = PeerDiscoveryTestHelper.generateDiscoveryPeers(keyPairs);

    // Mock the creation of the PING packet, so that we can control the hash, which gets validated
    // when
    // processing the PONG.
    final PingPacketData mockPing =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet mockPacket = Packet.create(PacketType.PING, mockPing, keyPairs[0]);

    // Initialize the peer controller, setting a high controller refresh interval and a high timeout
    // threshold, to avoid retries
    // getting in the way of this test.
    controller = createController(Arrays.asList(peers[0], peers[1]));
    doReturn(mockPacket).when(controller).createPacket(eq(PacketType.PING), any());

    controller.setRetryDelayFunction((prev) -> 999999999L);
    controller.start();

    verify(controller, times(1)).sendPacket(eq(peers[0]), eq(PacketType.PING), any());
    verify(controller, times(1)).sendPacket(eq(peers[1]), eq(PacketType.PING), any());

    // Simulate a PONG message from peer[0].
    final PongPacketData packetData =
        PongPacketData.create(self.getEndpoint(), mockPacket.getHash());
    Packet pongPacket = Packet.create(PacketType.PONG, packetData, keyPairs[0]);
    controller.onMessage(pongPacket, peers[0]);

    // Simulate a NEIGHBORS message from peer[0] listing peer[2].
    final NeighborsPacketData neighbors =
        NeighborsPacketData.create(Collections.singletonList(peers[2]));
    Packet neighborsPacket = Packet.create(PacketType.NEIGHBORS, neighbors, keyPairs[0]);
    controller.onMessage(neighborsPacket, peers[0]);

    // Assert that we're bonding with the third peer.
    assertThat(controller.getPeers()).hasSize(2);
    assertThat(controller.getPeers())
        .filteredOn(p -> p.getStatus() == PeerDiscoveryStatus.BONDING)
        .hasSize(1);
    assertThat(controller.getPeers())
        .filteredOn(p -> p.getStatus() == PeerDiscoveryStatus.BONDED)
        .hasSize(1);

    // Send a PONG packet from peer[2], to transition it to the BONDED state.
    pongPacket = Packet.create(PacketType.PONG, packetData, keyPairs[2]);
    controller.onMessage(pongPacket, peers[2]);

    // Assert we're now bonded with peer[2].
    assertThat(controller.getPeers())
        .filteredOn(p -> p.equals(peers[2]) && p.getStatus() == PeerDiscoveryStatus.BONDED)
        .hasSize(1);

    // Simulate bonding and neighbors packet from the second boostrap peer, with peer[2] reported in
    // the peer list.
    pongPacket = Packet.create(PacketType.PONG, packetData, keyPairs[1]);
    controller.onMessage(pongPacket, peers[1]);
    neighborsPacket = Packet.create(PacketType.NEIGHBORS, neighbors, keyPairs[1]);
    controller.onMessage(neighborsPacket, peers[1]);

    // Wait for 1 second and ensure that only 1 PING was ever sent to peer[2].
    Thread.sleep(1000);
    verify(controller, times(1)).sendPacket(eq(peers[2]), eq(PacketType.PING), any());
  }

  @Test(expected = IllegalStateException.class)
  public void startTwice() {
    startPeerDiscoveryController();
    controller.start();
  }

  @Test
  public void stopTwice() {
    startPeerDiscoveryController();
    controller.stop();
    controller.stop();
    // no exception
  }

  @Test
  public void shouldAddNewPeerWhenReceivedPingAndPeerTableBucketIsNotFull() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 1);
    startPeerDiscoveryController();

    final Packet pingPacket = mockPingPacket(peers[0], self);
    controller.onMessage(pingPacket, peers[0]);
    assertThat(controller.getPeers()).contains(peers[0]);
  }

  @Test
  public void shouldNotAddSelfWhenReceivedPingFromSelf() {
    startPeerDiscoveryController();
    final DiscoveryPeer self = new DiscoveryPeer(this.self.getId(), this.self.getEndpoint());

    final Packet pingPacket = mockPingPacket(this.self, this.self);
    controller.onMessage(pingPacket, self);

    assertThat(controller.getPeers()).doesNotContain(self);
  }

  @Test
  public void shouldAddNewPeerWhenReceivedPingAndPeerTableBucketIsFull() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 17);
    startPeerDiscoveryController();
    // Fill the last bucket.
    for (int i = 0; i < 16; i++) {
      peerTable.tryAdd(peers[i]);
    }

    final Packet pingPacket = mockPingPacket(peers[16], self);
    controller.onMessage(pingPacket, peers[16]);

    assertThat(controller.getPeers()).contains(peers[16]);
    // The first peer added should have been evicted.
    assertThat(controller.getPeers()).doesNotContain(peers[0]);
  }

  @Test
  public void shouldNotRemoveExistingPeerWhenReceivedPing() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 1);
    startPeerDiscoveryController();

    peerTable.tryAdd(peers[0]);
    assertThat(controller.getPeers()).contains(peers[0]);

    final Packet pingPacket = mockPingPacket(peers[0], self);
    controller.onMessage(pingPacket, peers[0]);

    assertThat(controller.getPeers()).contains(peers[0]);
  }

  @Test
  public void shouldNotAddNewPeerWhenReceivedPongFromBlacklistedPeer() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 3);
    final DiscoveryPeer discoPeer = peers[0];
    final DiscoveryPeer otherPeer = peers[1];
    final DiscoveryPeer otherPeer2 = peers[2];

    final PeerBlacklist blacklist = new PeerBlacklist();
    controller = createController(Collections.singletonList(discoPeer), blacklist);

    final Endpoint selfEndpoint = self.getEndpoint();

    // Setup ping to be sent to discoPeer
    SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    PingPacketData pingPacketData = PingPacketData.create(selfEndpoint, discoPeer.getEndpoint());
    final Packet discoPeerPing = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(discoPeerPing)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(discoPeer));

    controller.start();
    verify(controller, times(1)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(controller, times(1)).sendPacket(eq(discoPeer), eq(PacketType.FIND_NEIGHBORS), any());

    // Setup ping to be sent to otherPeer after neighbors packet is received
    keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    pingPacketData = PingPacketData.create(selfEndpoint, otherPeer.getEndpoint());
    final Packet pingPacket = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(pingPacket)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(otherPeer));

    // Setup ping to be sent to otherPeer2 after neighbors packet is received
    keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    pingPacketData = PingPacketData.create(selfEndpoint, otherPeer2.getEndpoint());
    final Packet pingPacket2 = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(pingPacket2)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(otherPeer2));

    final Packet neighborsPacket =
        MockPacketDataFactory.mockNeighborsPacket(discoPeer, otherPeer, otherPeer2);
    controller.onMessage(neighborsPacket, discoPeer);

    verify(controller, times(peers.length)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongPacket = MockPacketDataFactory.mockPongPacket(otherPeer, pingPacket.getHash());
    controller.onMessage(pongPacket, otherPeer);

    // Blacklist otherPeer2 before sending return pong
    blacklist.add(otherPeer2);
    final Packet pongPacket2 =
        MockPacketDataFactory.mockPongPacket(otherPeer2, pingPacket2.getHash());
    controller.onMessage(pongPacket2, otherPeer2);

    assertThat(controller.getPeers()).hasSize(2);
    assertThat(controller.getPeers()).contains(discoPeer);
    assertThat(controller.getPeers()).contains(otherPeer);
    assertThat(controller.getPeers()).doesNotContain(otherPeer2);
  }

  private PacketData matchPingDataForPeer(DiscoveryPeer peer) {
    return argThat((PacketData data) -> ((PingPacketData) data).getTo().equals(peer.getEndpoint()));
  }

  @Test
  public void shouldNotBondWithBlacklistedPeer()
      throws InterruptedException, ExecutionException, TimeoutException {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 3);
    final DiscoveryPeer discoPeer = peers[0];
    final DiscoveryPeer otherPeer = peers[1];
    final DiscoveryPeer otherPeer2 = peers[2];

    final PeerBlacklist blacklist = new PeerBlacklist();
    controller = createController(Collections.singletonList(discoPeer), blacklist);

    final Endpoint selfEndpoint = self.getEndpoint();

    // Setup ping to be sent to discoPeer
    SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    PingPacketData pingPacketData = PingPacketData.create(selfEndpoint, discoPeer.getEndpoint());
    final Packet discoPeerPing = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(discoPeerPing)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(discoPeer));

    controller.start();
    verify(controller, times(1)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(controller, times(1)).sendPacket(eq(discoPeer), eq(PacketType.FIND_NEIGHBORS), any());

    // Setup ping to be sent to otherPeer after neighbors packet is received
    keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    pingPacketData = PingPacketData.create(selfEndpoint, otherPeer.getEndpoint());
    final Packet pingPacket = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(pingPacket)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(otherPeer));

    // Setup ping to be sent to otherPeer2 after neighbors packet is received
    keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    pingPacketData = PingPacketData.create(selfEndpoint, otherPeer2.getEndpoint());
    final Packet pingPacket2 = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(pingPacket2)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(otherPeer2));

    // Blacklist peer
    blacklist.add(otherPeer);

    final Packet neighborsPacket =
        MockPacketDataFactory.mockNeighborsPacket(discoPeer, otherPeer, otherPeer2);
    controller.onMessage(neighborsPacket, discoPeer);

    verify(controller, times(0)).bond(otherPeer, false);
    verify(controller, times(1)).bond(otherPeer2, false);
  }

  @Test
  public void shouldRespondToNeighborsRequestFromKnownPeer()
      throws InterruptedException, ExecutionException, TimeoutException {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 1);
    final DiscoveryPeer discoPeer = peers[0];

    final PeerBlacklist blacklist = new PeerBlacklist();
    controller = createController(Collections.singletonList(discoPeer), blacklist);

    final Endpoint selfEndpoint = self.getEndpoint();

    // Setup ping to be sent to discoPeer
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    final PingPacketData pingPacketData =
        PingPacketData.create(selfEndpoint, discoPeer.getEndpoint());
    final Packet discoPeerPing = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(discoPeerPing)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(discoPeer));

    controller.start();
    verify(controller, times(1)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(controller, times(1)).sendPacket(eq(discoPeer), eq(PacketType.FIND_NEIGHBORS), any());

    final Packet findNeighborsPacket = MockPacketDataFactory.mockFindNeighborsPacket(discoPeer);
    controller.onMessage(findNeighborsPacket, discoPeer);

    verify(controller, times(1)).sendPacket(eq(discoPeer), eq(PacketType.NEIGHBORS), any());
  }

  @Test
  public void shouldNotRespondToNeighborsRequestFromUnknownPeer()
      throws InterruptedException, ExecutionException, TimeoutException {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 2);
    final DiscoveryPeer discoPeer = peers[0];
    final DiscoveryPeer otherPeer = peers[1];

    final PeerBlacklist blacklist = new PeerBlacklist();
    controller = spy(createController(Collections.singletonList(discoPeer), blacklist));

    final Endpoint selfEndpoint = self.getEndpoint();

    // Setup ping to be sent to discoPeer
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    final PingPacketData pingPacketData =
        PingPacketData.create(selfEndpoint, discoPeer.getEndpoint());
    final Packet discoPeerPing = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(discoPeerPing)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(discoPeer));

    controller.start();
    verify(controller, times(1)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(controller, times(1)).sendPacket(eq(discoPeer), eq(PacketType.FIND_NEIGHBORS), any());

    final Packet findNeighborsPacket = MockPacketDataFactory.mockFindNeighborsPacket(discoPeer);
    controller.onMessage(findNeighborsPacket, otherPeer);

    verify(controller, times(0)).sendPacket(eq(otherPeer), eq(PacketType.NEIGHBORS), any());
  }

  @Test
  public void shouldNotRespondToNeighborsRequestFromBlacklistedPeer()
      throws InterruptedException, ExecutionException, TimeoutException {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 1);
    final DiscoveryPeer discoPeer = peers[0];

    final PeerBlacklist blacklist = new PeerBlacklist();
    controller = createController(Collections.singletonList(discoPeer), blacklist);

    final Endpoint selfEndpoint = self.getEndpoint();

    // Setup ping to be sent to discoPeer
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    final PingPacketData pingPacketData =
        PingPacketData.create(selfEndpoint, discoPeer.getEndpoint());
    final Packet discoPeerPing = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(discoPeerPing)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(discoPeer));

    controller.start();
    verify(controller, times(1)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(controller, times(1)).sendPacket(eq(discoPeer), eq(PacketType.FIND_NEIGHBORS), any());

    blacklist.add(discoPeer);
    final Packet findNeighborsPacket = MockPacketDataFactory.mockFindNeighborsPacket(discoPeer);
    controller.onMessage(findNeighborsPacket, discoPeer);

    verify(controller, times(0)).sendPacket(eq(discoPeer), eq(PacketType.NEIGHBORS), any());
  }

  @Test
  public void shouldAddNewPeerWhenReceivedPongAndPeerTableBucketIsNotFull() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 1);

    // Mock the creation of the PING packet to control hash for PONG.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    final PingPacketData pingPacketData =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet pingPacket = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);

    controller = createController(Arrays.asList(peers[0]));
    doReturn(pingPacket).when(controller).createPacket(eq(PacketType.PING), any());

    controller.setRetryDelayFunction((prev) -> 999999999L);
    controller.start();

    verify(controller, times(1)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongPacket = MockPacketDataFactory.mockPongPacket(peers[0], pingPacket.getHash());
    controller.onMessage(pongPacket, peers[0]);

    assertThat(controller.getPeers()).contains(peers[0]);
  }

  @Test
  public void shouldAddNewPeerWhenReceivedPongAndPeerTableBucketIsFull() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 17);

    final DiscoveryPeer[] bootstrapPeers = Arrays.copyOfRange(peers, 0, 16);
    controller = createController(Arrays.asList(bootstrapPeers));
    controller.setRetryDelayFunction(LONG_DELAY_FUNCTION);

    // Mock the creation of PING packets to control hash PONG packets.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    final PingPacketData pingPacketData =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet pingPacket = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(pingPacket).when(controller).createPacket(eq(PacketType.PING), any());

    controller.start();

    verify(controller, times(16)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongPacket = MockPacketDataFactory.mockPongPacket(peers[0], pingPacket.getHash());
    controller.onMessage(pongPacket, peers[0]);

    final Packet neighborsPacket = MockPacketDataFactory.mockNeighborsPacket(peers[0], peers[16]);
    controller.onMessage(neighborsPacket, peers[0]);

    final Packet pongPacket2 =
        MockPacketDataFactory.mockPongPacket(peers[16], pingPacket.getHash());
    controller.onMessage(pongPacket2, peers[16]);

    assertThat(controller.getPeers()).contains(peers[16]);
    // Explain
    assertThat(controller.getPeers()).doesNotContain(peers[1]);
  }

  @Test
  public void shouldNotAddPeerInNeighborsPacketWithoutBonding() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 2);

    // Mock the creation of the PING packet to control hash for PONG.
    final SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    final PingPacketData pingPacketData =
        PingPacketData.create(self.getEndpoint(), peers[0].getEndpoint());
    final Packet pingPacket = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);

    PeerDiscoveryController controller = createController(peers[0]);
    doReturn(pingPacket).when(controller).createPacket(eq(PacketType.PING), any());
    controller.start();

    verify(controller, times(1)).sendPacket(eq(peers[0]), eq(PacketType.PING), any());

    final Packet pongPacket = MockPacketDataFactory.mockPongPacket(peers[0], pingPacket.getHash());
    controller.onMessage(pongPacket, peers[0]);

    verify(controller, times(1)).sendPacket(eq(peers[0]), eq(PacketType.FIND_NEIGHBORS), any());

    assertThat(controller.getPeers()).doesNotContain(peers[1]);
  }

  @Test
  public void shouldNotBondWithNonWhitelistedPeer()
      throws InterruptedException, ExecutionException, TimeoutException {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 3);
    final DiscoveryPeer discoPeer = peers[0];
    final DiscoveryPeer otherPeer = peers[1];
    final DiscoveryPeer otherPeer2 = peers[2];

    final PeerBlacklist blacklist = new PeerBlacklist();
    final PermissioningConfiguration config = new PermissioningConfiguration();
    NodeWhitelistController nodeWhitelistController = new NodeWhitelistController(config);

    // Whitelist peers
    nodeWhitelistController.addNode(discoPeer);
    nodeWhitelistController.addNode(otherPeer2);

    controller =
        createController(Collections.singletonList(discoPeer), blacklist, nodeWhitelistController);

    final Endpoint selfEndpoint = self.getEndpoint();

    // Setup ping to be sent to discoPeer
    SECP256K1.KeyPair[] keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    PingPacketData pingPacketData = PingPacketData.create(selfEndpoint, discoPeer.getEndpoint());
    final Packet discoPeerPing = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(discoPeerPing)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(discoPeer));

    controller.start();
    verify(controller, times(1)).sendPacket(any(), eq(PacketType.PING), any());

    final Packet pongFromDiscoPeer =
        MockPacketDataFactory.mockPongPacket(discoPeer, discoPeerPing.getHash());
    controller.onMessage(pongFromDiscoPeer, discoPeer);

    verify(controller, times(1)).sendPacket(eq(discoPeer), eq(PacketType.FIND_NEIGHBORS), any());

    // Setup ping to be sent to otherPeer after neighbors packet is received
    keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    pingPacketData = PingPacketData.create(selfEndpoint, otherPeer.getEndpoint());
    final Packet pingPacket = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(pingPacket)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(otherPeer));

    // Setup ping to be sent to otherPeer2 after neighbors packet is received
    keyPairs = PeerDiscoveryTestHelper.generateKeyPairs(1);
    pingPacketData = PingPacketData.create(selfEndpoint, otherPeer2.getEndpoint());
    final Packet pingPacket2 = Packet.create(PacketType.PING, pingPacketData, keyPairs[0]);
    doReturn(pingPacket2)
        .when(controller)
        .createPacket(eq(PacketType.PING), matchPingDataForPeer(otherPeer2));

    final Packet neighborsPacket =
        MockPacketDataFactory.mockNeighborsPacket(discoPeer, otherPeer, otherPeer2);
    controller.onMessage(neighborsPacket, discoPeer);

    verify(controller, times(0)).bond(otherPeer, false);
    verify(controller, times(1)).bond(otherPeer2, false);
  }

  @Test
  public void shouldNotRespondToPingFromNonWhitelistedDiscoveryPeer() {
    final DiscoveryPeer[] peers = createPeersInLastBucket(self, 3);
    final DiscoveryPeer discoPeer = peers[0];

    final PeerBlacklist blacklist = new PeerBlacklist();

    // don't add disco peer to whitelist
    PermissioningConfiguration config = PermissioningConfiguration.createDefault();
    config.setNodeWhitelist(new ArrayList<>());
    NodeWhitelistController nodeWhitelistController = new NodeWhitelistController(config);

    controller =
        createController(Collections.singletonList(discoPeer), blacklist, nodeWhitelistController);

    final Packet pingPacket = mockPingPacket(peers[0], self);
    controller.onMessage(pingPacket, peers[0]);
    assertThat(controller.getPeers()).doesNotContain(peers[0]);
  }

  private static Packet mockPingPacket(final Peer from, final Peer to) {
    final Packet packet = mock(Packet.class);

    final PingPacketData pingPacketData =
        PingPacketData.create(from.getEndpoint(), to.getEndpoint());
    when(packet.getPacketData(any())).thenReturn(Optional.of(pingPacketData));
    final BytesValue id = from.getId();
    when(packet.getNodeId()).thenReturn(id);
    when(packet.getType()).thenReturn(PacketType.PING);
    when(packet.getHash()).thenReturn(Bytes32.ZERO);

    return packet;
  }

  private DiscoveryPeer[] createPeersInLastBucket(final Peer host, final int n) {
    final DiscoveryPeer[] newPeers = new DiscoveryPeer[n];

    // Flipping the most significant bit of the keccak256 will place the peer
    // in the last bucket for the corresponding host peer.
    final Bytes32 keccak256 = host.keccak256();
    final MutableBytesValue template = MutableBytesValue.create(keccak256.size());
    byte msb = keccak256.get(0);
    msb ^= MOST_SIGNFICANT_BIT_MASK;
    template.set(0, msb);

    for (int i = 0; i < n; i++) {
      template.setInt(template.size() - 4, i);
      final Bytes32 newKeccak256 = Bytes32.leftPad(template.copy());
      final DiscoveryPeer newPeer = mock(DiscoveryPeer.class);
      when(newPeer.keccak256()).thenReturn(newKeccak256);
      final MutableBytesValue newId = MutableBytesValue.create(64);
      UInt256.of(i).getBytes().copyTo(newId, newId.size() - UInt256Value.SIZE);
      when(newPeer.getId()).thenReturn(newId);
      when(newPeer.getEndpoint())
          .thenReturn(
              new Endpoint(
                  host.getEndpoint().getHost(),
                  100 + counter.incrementAndGet(),
                  OptionalInt.empty()));
      newPeers[i] = newPeer;
    }

    return newPeers;
  }

  private PeerDiscoveryController startPeerDiscoveryController(
      final DiscoveryPeer... bootstrapPeers) {
    return startPeerDiscoveryController(LONG_DELAY_FUNCTION, bootstrapPeers);
  }

  private PeerDiscoveryController startPeerDiscoveryController(
      final RetryDelayFunction retryDelayFunction, final DiscoveryPeer... bootstrapPeers) {
    // Create the controller.
    controller = createController(Arrays.asList(bootstrapPeers));
    controller.setRetryDelayFunction(retryDelayFunction);
    controller.start();
    return controller;
  }

  static class ControllerBuilder {
    private Collection<DiscoveryPeer> discoPeers = Collections.emptyList();
    private PeerBlacklist blacklist = new PeerBlacklist();
    private NodeWhitelistController whitelist =
        new NodeWhitelistController(PermissioningConfiguration.createDefault());
    private MockTimerUtil timerUtil = new MockTimerUtil();
    private KeyPair keypair;
    private DiscoveryPeer self;
    private PeerTable peerTable;

    public static ControllerBuilder create() {
      return new ControllerBuilder();
    }

    ControllerBuilder setPeers(Collection<DiscoveryPeer> discoPeers) {
      this.discoPeers = discoPeers;
      return this;
    }

    ControllerBuilder setPeers(DiscoveryPeer... discoPeers) {
      this.discoPeers = Arrays.asList(discoPeers);
      return this;
    }

    ControllerBuilder setBlacklist(PeerBlacklist blacklist) {
      this.blacklist = blacklist;
      return this;
    }

    ControllerBuilder setWhitelist(NodeWhitelistController whitelist) {
      this.whitelist = whitelist;
      return this;
    }

    ControllerBuilder setTimerUtil(MockTimerUtil timerUtil) {
      this.timerUtil = timerUtil;
      return this;
    }

    ControllerBuilder setKeyPair(KeyPair keypair) {
      this.keypair = keypair;
      return this;
    }

    ControllerBuilder setSelf(DiscoveryPeer self) {
      this.self = self;
      return this;
    }

    ControllerBuilder setPeerTable(PeerTable peerTable) {
      this.peerTable = peerTable;
      return this;
    }

    PeerDiscoveryController build() {
      checkNotNull(keypair);
      if (self == null) {
        self = PeerDiscoveryTestHelper.generatePeers(keypair)[0];
      }
      if (peerTable == null) {
        peerTable = new PeerTable(self.getId());
      }
      return spy(
          new PeerDiscoveryController(
              timerUtil,
              keypair,
              self,
              peerTable,
              discoPeers,
              TABLE_REFRESH_INTERVAL_MS,
              PEER_REQUIREMENT,
              blacklist,
              whitelist,
              OutboundMessageHandler.NOOP,
              new Subscribers<>()));
    }
  }
}
