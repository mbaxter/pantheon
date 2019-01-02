package tech.pegasys.pantheon.ethereum.p2p.discovery.internal;

import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;

@FunctionalInterface
public interface OutboundMessageHandler {
  public static OutboundMessageHandler NOOP = (peer, packet) -> {};

  void send(final DiscoveryPeer peer, final Packet packet);
}
