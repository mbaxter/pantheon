package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;

@FunctionalInterface
public interface MessageCallback {
  MessageCallback NOOP = (final PeerConnection connection, final Capability capability,
  final MessageData message) -> {};

  void onMessage(final PeerConnection connection, final Capability capability,
    final MessageData message);
}
