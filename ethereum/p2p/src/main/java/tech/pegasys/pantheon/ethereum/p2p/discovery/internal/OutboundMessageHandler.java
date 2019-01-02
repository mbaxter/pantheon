package tech.pegasys.pantheon.ethereum.p2p.discovery.internal;

import tech.pegasys.pantheon.ethereum.p2p.discovery.DiscoveryPeer;

@FunctionalInterface
public interface OutboundMessageHandler {
  Packet send(final DiscoveryPeer peer, final PacketType type, final PacketData data);
}
