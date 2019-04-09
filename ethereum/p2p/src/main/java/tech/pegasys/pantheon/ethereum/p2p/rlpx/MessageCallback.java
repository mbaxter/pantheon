package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;

public interface MessageCallback {
  void onMessage(final PeerConnection connection, final Capability capability,
    final MessageData message);
}
