package tech.pegasys.pantheon.ethereum.p2p.peers;

import io.vertx.core.impl.ConcurrentHashSet;
import java.util.OptionalInt;
import java.util.Set;
import tech.pegasys.pantheon.util.LimitedSet;
import tech.pegasys.pantheon.util.bytes.BytesValue;

public class PeerPermissionsBlacklist extends PeerPermissions {

  private final Set<BytesValue> blacklist;

  private PeerPermissionsBlacklist(OptionalInt maxSize) {
    if (maxSize.isPresent()) {
      blacklist = LimitedSet.create(maxSize.getAsInt());
    } else {
      blacklist = new ConcurrentHashSet<>();
    }
  }

  public static PeerPermissionsBlacklist create() {
    return new PeerPermissionsBlacklist(OptionalInt.empty());
  }

  public static PeerPermissionsBlacklist create(int maxSize) {
    return new PeerPermissionsBlacklist(OptionalInt.of(maxSize));
  }

  @Override
  public boolean isPermitted(final Peer peer) {
    return !blacklist.contains(peer.getId());
  }

  @Override
  public boolean isReady() {
    return true;
  }

  public void add(Peer peer) {
    blacklist.add(peer.getId());
    dispatchUpdate();
  }

  public void remove(Peer peer) {
    blacklist.remove(peer.getId());
    dispatchUpdate();
  }

  public void add(BytesValue peerId) {
    blacklist.add(peerId);
    dispatchUpdate();
  }

  public void remove(BytesValue peerId) {
    blacklist.remove(peerId);
    dispatchUpdate();
  }

}
