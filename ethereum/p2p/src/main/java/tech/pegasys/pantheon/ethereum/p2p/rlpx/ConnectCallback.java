package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;

public interface ConnectCallback {
  void onConnect(final PeerConnection peer, final boolean initiatedByPeer);
}
