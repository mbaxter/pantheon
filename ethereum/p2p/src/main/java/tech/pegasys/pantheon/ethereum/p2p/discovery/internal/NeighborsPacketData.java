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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.rlp.RLPInput;
import tech.pegasys.pantheon.ethereum.rlp.RLPOutput;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.net.InetAddress;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NeighborsPacketData implements PacketData {

  private final List<Neighbor> neighbors;

  /* In millis after epoch. */
  private final long expiration;

  private NeighborsPacketData(final List<Neighbor> neighbors, final long expiration) {
    checkArgument(neighbors != null, "peer list cannot be null");
    checkArgument(expiration >= 0, "expiration must be positive");

    this.neighbors = neighbors;
    this.expiration = expiration;
  }

  @SuppressWarnings("unchecked")
  public static NeighborsPacketData create(final List<? extends Peer> peers) {
    List<Neighbor> neighbors =
        peers.stream().map(Peer::getEnodeURL).map(Neighbor::create).collect(Collectors.toList());
    return new NeighborsPacketData(
        neighbors, System.currentTimeMillis() + PacketData.DEFAULT_EXPIRATION_PERIOD_MS);
  }

  public static NeighborsPacketData readFrom(final RLPInput in) {
    in.enterList();
    final List<Neighbor> neighbors = in.readList(Neighbor::readFrom);
    final long expiration = in.readLongScalar();
    in.leaveList();
    return new NeighborsPacketData(neighbors, expiration);
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();
    out.writeList(neighbors, Neighbor::writeTo);
    out.writeLongScalar(expiration);
    out.endList();
  }

  public List<? extends Peer> getNodes() {
    return getNodes(DefaultPeer::fromEnodeURL);
  }

  public <T extends Peer> List<T> getNodes(final Function<EnodeURL, T> peerFactory) {
    return neighbors.stream().map(Neighbor::getEnode).map(peerFactory).collect(Collectors.toList());
  }

  public long getExpiration() {
    return expiration;
  }

  @Override
  public String toString() {
    return String.format("NeighborsPacketData{peers=%s, expiration=%d}", neighbors, expiration);
  }

  private static class Neighbor {
    // Neighbors packets are serialized inconsistently on the network.
    // Specify the serialization style so we can be sure serialization is consistent.
    enum SerializationMode {
      WRITE_TCP_PORT,
      NULL_TCP_PORT,
      SKIP_TCP_PORT
    }

    private final EnodeURL enode;
    private final SerializationMode mode;

    private Neighbor(final EnodeURL enode, final SerializationMode mode) {
      checkNotNull(enode);
      checkNotNull(mode);
      checkArgument(
          mode.equals(SerializationMode.WRITE_TCP_PORT) || !enode.getDiscoveryPort().isPresent(),
          "Invalid serialization mode: if discovery port is distinct from listening port, both ports must be written.");
      this.enode = enode;
      this.mode = mode;
    }

    public static Neighbor create(final EnodeURL enode) {
      return new Neighbor(enode, SerializationMode.WRITE_TCP_PORT);
    }

    public static Neighbor readFrom(final RLPInput in) {
      final SerializationMode mode;
      final int size = in.enterList();

      final InetAddress addr = in.readInetAddress();
      final int udpPort = in.readUnsignedShort();

      // A second port, if specified, represents a tcp listening port distinct from the discovery
      // port
      OptionalInt tcpPort = OptionalInt.empty();
      if (size == 4) {
        // Discovery and listening ports were serialized
        if (in.nextIsNull()) {
          in.skipNext();
          mode = SerializationMode.NULL_TCP_PORT;
        } else {
          tcpPort = OptionalInt.of(in.readUnsignedShort());
          mode = SerializationMode.WRITE_TCP_PORT;
        }
      } else {
        // If we have less than 4 items, the tcp port was not serialized
        mode = SerializationMode.SKIP_TCP_PORT;
      }

      final BytesValue id = in.readBytesValue();
      in.leaveList();

      final EnodeURL enode =
          EnodeURL.builder()
              .nodeId(id)
              .ipAddress(addr)
              .listeningPort(tcpPort.orElse(udpPort))
              .discoveryPort(udpPort)
              .build();

      return new Neighbor(enode, mode);
    }

    void writeTo(final RLPOutput out) {
      out.startList();
      out.writeInetAddress(enode.getIp());
      out.writeUnsignedShort(enode.getEffectiveDiscoveryPort());
      switch (mode) {
        case WRITE_TCP_PORT:
          out.writeUnsignedShort(enode.getListeningPort());
          break;
        case NULL_TCP_PORT:
          out.writeNull();
          break;
        case SKIP_TCP_PORT:
          // Nothing to write
          break;
      }
      out.writeBytesValue(enode.getNodeId());
      out.endList();
    }

    public EnodeURL getEnode() {
      return enode;
    }
  }
}
