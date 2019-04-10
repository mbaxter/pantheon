package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;

@FunctionalInterface
public interface DisconnectCallback {
  DisconnectCallback NOOP = (final PeerConnection peer, final DisconnectReason reason,
    final boolean initiatedByPeer) -> {};

  void onDisconnect(final PeerConnection peer, final DisconnectReason reason,
    final boolean initiatedByPeer);
}
