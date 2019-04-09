package tech.pegasys.pantheon.ethereum.p2p.peers;

public interface MutableLocalNode extends LocalNode {
  void set(Peer peer);
}
