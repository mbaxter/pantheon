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
package tech.pegasys.pantheon.ethereum.p2p.peers;

import tech.pegasys.pantheon.ethereum.rlp.RLPInput;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.net.InetAddress;
import java.net.URI;
import java.util.Objects;
import java.util.OptionalInt;

/** The default, basic representation of an Ethereum {@link Peer}. */
public class DefaultPeer extends DefaultPeerId implements Peer {

  private final EnodeURL enode;

  protected DefaultPeer(final EnodeURL enode) {
    super(enode.getNodeId());
    this.enode = enode;
  }

  public static DefaultPeer fromEnodeURL(final EnodeURL enodeURL) {
    return new DefaultPeer(enodeURL);
  }

  /**
   * Creates a {@link DefaultPeer} instance from a String representation of an enode URL.
   *
   * @param uri A String representation of the enode URI.
   * @return The Peer instance.
   * @see <a href="https://github.com/ethereum/wiki/wiki/enode-url-format">enode URL format</a>
   */
  public static DefaultPeer fromURI(final String uri) {
    return new DefaultPeer(EnodeURL.fromString(uri));
  }

  /**
   * Creates a {@link DefaultPeer} instance from an URI object that follows the enode URL format.
   *
   * @param uri The enode URI.
   * @return The Peer instance.
   * @see <a href="https://github.com/ethereum/wiki/wiki/enode-url-format">enode URL format</a>
   */
  public static DefaultPeer fromURI(final URI uri) {
    return new DefaultPeer(EnodeURL.fromURI(uri));
  }

  /**
   * Decodes the RLP stream as a Peer instance.
   *
   * @param in The RLP input stream from which to read.
   * @return The decoded representation.
   */
  public static Peer readFrom(final RLPInput in) {
    final int size = in.enterList();

    final InetAddress addr = in.readInetAddress();
    final int udpPort = in.readUnsignedShort();
    // A second port, if specified, represents a tcp listening port distinct from the discovery port
    OptionalInt tcpPort = OptionalInt.empty();
    if (size == 4) {
      // Discovery and listening ports were serialized
      if (in.nextIsNull()) {
        in.skipNext();
      } else {
        tcpPort = OptionalInt.of(in.readUnsignedShort());
      }
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

    return new DefaultPeer(enode);
  }

  @Override
  public EnodeURL getEnodeURL() {
    return enode;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof DefaultPeer)) {
      return false;
    }
    final DefaultPeer other = (DefaultPeer) obj;
    return id.equals(other.id) && enode.equals(other.enode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, enode);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DefaultPeer{");
    sb.append("id=").append(id);
    sb.append(", enode=").append(enode);
    sb.append('}');
    return sb.toString();
  }
}
