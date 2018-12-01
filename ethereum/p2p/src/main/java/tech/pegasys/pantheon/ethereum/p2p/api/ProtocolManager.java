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

import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.PeerRequirement;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;

import java.util.List;
import java.util.Optional;

/** Represents an object responsible for managing a wire subprotocol. */
public interface ProtocolManager extends AutoCloseable, PeerRequirement {

  String getSupportedProtocol();

  /**
   * Defines the list of capabilities supported by this manager.
   *
   * @return the list of capabilities supported by this manager
   */
  List<Capability> getSupportedCapabilities();

  /** Stops the protocol manager. */
  void stop();

  /**
   * Blocks until protocol manager has stopped.
   *
   * @throws InterruptedException if interrupted while waiting
   */
  void awaitStop() throws InterruptedException;

  /**
   * Processes a message from a peer.
   *
   * @param cap the capability that corresponds to the message
   * @param message the message from the peer
   */
  void processMessage(Capability cap, Message message);

  /**
   * Handles new peer connections.
   *
   * @param peerConnection the new peer connection
   */
  void handleNewConnection(PeerConnection peerConnection);

  /**
   * Handles peer disconnects.
   *
   * @param peerConnection the connection that is being closed
   * @param disconnectReason the reason given for closing the connection
   * @param initiatedByPeer true if the peer requested to disconnect, false if this node requested
   */
  void handleDisconnect(
      PeerConnection peerConnection,
      Optional<DisconnectReason> disconnectReason,
      boolean initiatedByPeer);

  @Override
  default void close() {
    stop();
  }
}
