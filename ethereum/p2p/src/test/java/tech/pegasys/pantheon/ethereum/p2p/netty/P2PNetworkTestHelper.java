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

import static org.mockito.Mockito.spy;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

public class P2PNetworkTestHelper {
  private static AtomicInteger nextPort = new AtomicInteger(30303);

  public static Peer mockPeer() {
    return mockPeer(Peer.randomId(), "127.0.0.1", nextPort.incrementAndGet());
  }

  public static Peer mockPeer(final String host, final int port) {
    final BytesValue id = SECP256K1.KeyPair.generate().getPublicKey().getEncodedBytes();
    return mockPeer(id, host, port);
  }

  public static Peer mockPeer(final BytesValue id, final String host, final int port) {
    final EnodeURL enode = new EnodeURL(id, host, port, OptionalInt.empty());
    return DefaultPeer.fromEnodeURL(enode);
  }

  public static PeerConnection mockPeerConnection() {
    return mockPeerConnection(mockPeer());
  }

  public static PeerConnection mockPeerConnection(final Peer peer) {
    return spy(MockP2PNetwork.createMockPeerConnection(peer));
  }

  public static List<Disconnection> collectDisconnects(final P2PNetwork network) {
    List<Disconnection> disconnections = new ArrayList<>();
    network.subscribeDisconnect(
        (connection, reason, initiatedByPeer) ->
            disconnections.add(Disconnection.create(connection, reason, initiatedByPeer)));
    return disconnections;
  }

  public static class Disconnection {
    private final PeerConnection connection;
    private final DisconnectReason reason;
    private final boolean initiatedByPeer;

    private Disconnection(
        final PeerConnection connection,
        final DisconnectReason reason,
        final boolean initiatedByPeer) {
      this.connection = connection;
      this.reason = reason;
      this.initiatedByPeer = initiatedByPeer;
    }

    public static Disconnection create(
        final PeerConnection connection,
        final DisconnectReason reason,
        final boolean initiatedByPeer) {
      return new Disconnection(connection, reason, initiatedByPeer);
    }

    public PeerConnection getConnection() {
      return connection;
    }

    public DisconnectReason getReason() {
      return reason;
    }

    public boolean isInitiatedByPeer() {
      return initiatedByPeer;
    }
  }
}
