package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;

@FunctionalInterface
public interface ConnectCallback {
  ConnectCallback NOOP = (final PeerConnection peer, final boolean initiatedByPeer) -> {};

  void onConnect(final PeerConnection peer, final boolean initiatedByPeer);
}
