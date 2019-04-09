package tech.pegasys.pantheon.ethereum.p2p.rlpx.peers;

import java.util.stream.Stream;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;

@FunctionalInterface
public interface AvailablePeersSupplier {
  Stream<? extends Peer> get();
}
