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
package tech.pegasys.pantheon.ethereum.p2p.api;

import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

/** A P2P connection to another node. */
public interface PeerConnection {

  /**
   * Send given data to the connected node.
   *
   * @param message Data to send
   * @param capability Sub-protocol to use
   * @throws PeerNotConnected On attempt to send to a disconnected peer
   */
  void send(Capability capability, MessageData message) throws PeerNotConnected;

  /** @return a list of shared capabilities between this node and the connected peer */
  Set<Capability> getAgreedCapabilities();

  /**
   * Returns the agreed capability corresponding to given protocol.
   *
   * @param protocol the name of the protocol
   * @return the agreed capability corresponding to this protocol, returns null if no matching
   *     capability is supported
   */
  default Capability capability(final String protocol) {
    for (final Capability cap : getAgreedCapabilities()) {
      if (cap.getName().equalsIgnoreCase(protocol)) {
        return cap;
      }
    }
    return null;
  }

  /**
   * Sends a message to the peer for the given subprotocol
   *
   * @param protocol the subprotocol name
   * @param message the message to send
   * @throws PeerNotConnected if the peer has disconnected
   */
  default void sendForProtocol(final String protocol, final MessageData message)
      throws PeerNotConnected {
    send(capability(protocol), message);
  }

  /** @return The unix timestamp representing the time at which this connection was established. */
  long getConnectedAt();

  /**
   * Returns the Peer's Description.
   *
   * @return Peer Description
   */
  PeerInfo getPeerInfo();

  /** @return The peer associated with this connection. */
  Peer getPeer();

  /**
   * Immediately terminate the connection without sending a disconnect message.
   *
   * @param reason the reason for disconnection
   * @param peerInitiated <code>true</code> if and only if the remote peer requested disconnection
   */
  void terminateConnection(DisconnectReason reason, boolean peerInitiated);

  /**
   * Disconnect from this Peer.
   *
   * @param reason Reason for disconnecting
   */
  void disconnect(DisconnectReason reason);

  /** @return True if the peer is disconnected */
  boolean isDisconnected();

  InetSocketAddress getLocalAddress();

  InetSocketAddress getRemoteAddress();

  default EnodeURL getRemoteEnodeURL() {
    final PeerInfo peerInfo = getPeerInfo();
    final String nodeId = peerInfo.getNodeId().toString().substring(2);
    final int localPort = peerInfo.getPort();
    return new EnodeURL(nodeId, getRemoteAddress().getAddress(), localPort);
  }

  class PeerNotConnected extends IOException {

    public PeerNotConnected(final String message) {
      super(message);
    }
  }

  default boolean isRemoteEnode(final EnodeURL remoteEnodeUrl) {
    return ((remoteEnodeUrl.getNodeId().equals(this.getPeerInfo().getAddress()))
        && (remoteEnodeUrl.getListeningPort() == this.getPeerInfo().getPort())
        && (remoteEnodeUrl.getInetAddress().equals(this.getRemoteAddress().getAddress())));
  }
}
