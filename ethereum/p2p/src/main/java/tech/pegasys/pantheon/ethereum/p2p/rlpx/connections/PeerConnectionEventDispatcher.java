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
package tech.pegasys.pantheon.ethereum.p2p.rlpx.connections;

import tech.pegasys.pantheon.ethereum.p2p.api.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.DefaultMessage;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;

public class PeerConnectionEventDispatcher {
  public static PeerConnectionEventDispatcher NOOP =
      new PeerConnectionEventDispatcher(DisconnectCallback.NOOP, MessageCallback.NOOP);

  private final DisconnectCallback disconnectDispatcher;
  private final MessageCallback messageDispatcher;

  public PeerConnectionEventDispatcher(
      final DisconnectCallback disconnectDispatcher, final MessageCallback messageDispatcher) {
    this.disconnectDispatcher = disconnectDispatcher;
    this.messageDispatcher = messageDispatcher;
  }

  public void dispatchPeerDisconnected(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    disconnectDispatcher.onDisconnect(connection, reason, initiatedByPeer);
  }

  public void dispatchMessageReceived(
      final Capability capability, final PeerConnection connection, final MessageData message) {
    messageDispatcher.onMessage(capability, new DefaultMessage(connection, message));
  }
}
