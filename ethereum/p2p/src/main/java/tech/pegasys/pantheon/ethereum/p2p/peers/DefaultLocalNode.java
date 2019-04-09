package tech.pegasys.pantheon.ethereum.p2p.peers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import tech.pegasys.pantheon.util.Subscribers;

/**
 * Represents the local network node.
 */
public class DefaultLocalNode implements MutableLocalNode {
  private Optional<Peer> localNode = Optional.empty();
  private Subscribers<ReadyCallback> readySubscribers = new Subscribers<>();

  @Override
  public Optional<Peer> getPeer() {
    return localNode;
  }

  @Override
  public boolean isReady() {
    return getPeer().isPresent();
  }

  @Override
  public synchronized void subscribeReady(ReadyCallback callback) {
    if (isReady()) {
      callback.onReady(localNode.get());
    } else {
      readySubscribers.subscribe(callback);
    }
  }

  @Override
  public void set(Peer peer) {
    if (localNode.isPresent()) {
      throw new IllegalStateException("Attempt to set already initialized local node");
    }
    localNode = Optional.of(peer);
    dispatchReady(peer);
  }

  private synchronized void dispatchReady(Peer localNode) {
    checkNotNull(localNode);
    readySubscribers.forEach(c -> c.onReady(localNode));
    readySubscribers.clear();
  }
}
