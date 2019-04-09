package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;

public interface DisconnectCallback {
  void onDisconnect(final PeerConnection peer, final DisconnectReason reason,
    final boolean initiatedByPeer);
}
