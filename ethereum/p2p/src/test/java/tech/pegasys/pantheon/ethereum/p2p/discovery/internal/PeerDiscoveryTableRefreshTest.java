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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryTestHelper;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.permissioning.NodeWhitelistController;
import tech.pegasys.pantheon.ethereum.permissioning.PermissioningConfiguration;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PeerDiscoveryTableRefreshTest {
  @Test
  public void tableRefreshSingleNode() {
    final SECP256K1.KeyPair[] keypairs = PeerDiscoveryTestHelper.generateKeyPairs(2);
    final DiscoveryPeer[] peers = PeerDiscoveryTestHelper.generateDiscoveryPeers(keypairs);
    DiscoveryPeer self = peers[0];
    KeyPair selfKeyPair = keypairs[0];

    // Create and start the PeerDiscoveryController
    MockTimerUtil timer = new MockTimerUtil();
    final PeerDiscoveryController controller =
        spy(
            new PeerDiscoveryController(
                timer,
                selfKeyPair,
                self,
                new PeerTable(self.getId()),
                emptyList(),
                0,
                () -> true,
                new PeerBlacklist(),
                new NodeWhitelistController(PermissioningConfiguration.createDefault()),
                OutboundMessageHandler.NOOP,
                new Subscribers<>()));
    controller.start();

    // Send a PING, so as to add a Peer in the controller.
    final PingPacketData ping =
        PingPacketData.create(peers[1].getEndpoint(), peers[0].getEndpoint());
    final Packet packet = Packet.create(PacketType.PING, ping, keypairs[1]);
    controller.onMessage(packet, peers[1]);

    // Wait until the controller has added the newly found peer.
    assertThat(controller.getPeers()).hasSize(1);

    // As the controller performs refreshes, it'll send FIND_NEIGHBORS packets with random target
    // IDs every time.
    // We capture the packets so that we can later assert on them.
    // Within 1000ms, there should be ~10 packets. But let's be less ambitious and expect at least
    // 5.
    final ArgumentCaptor<PacketData> packetDataCaptor = ArgumentCaptor.forClass(PacketData.class);
    for (int i = 0; i < 5; i++) {
      timer.runPeriodicHandlers();
    }
    verify(controller, times(5))
        .sendPacket(eq(peers[1]), eq(PacketType.FIND_NEIGHBORS), packetDataCaptor.capture());

    // Assert that all packets were FIND_NEIGHBORS packets.
    final List<BytesValue> targets = new ArrayList<>();
    for (final PacketData data : packetDataCaptor.getAllValues()) {
      assertThat(data).isExactlyInstanceOf(FindNeighborsPacketData.class);
      final FindNeighborsPacketData neighborsData = (FindNeighborsPacketData) data;
      targets.add(neighborsData.getTarget());
    }

    assertThat(targets.size()).isEqualTo(5);

    // All targets are unique.
    assertThat(targets.size()).isEqualTo(new HashSet<>(targets).size());
  }
}
