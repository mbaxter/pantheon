package tech.pegasys.pantheon.ethereum.p2p.peers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import tech.pegasys.pantheon.util.Subscribers;

/**
 * Represents the local network node.
 */
public interface LocalNode {

  /**
   * While node is initializing, an empty value will be returned.
   * Once this node is up and running, a {@link Peer} object corresponding to this node will
   * be returned.
   * @return The {@link Peer} representation associated with this node.
   */
  Optional<Peer> getPeer();

  /**
   * @return True if the local node is up and running and has an available {@link Peer} representation.
   */
  boolean isReady();

  /**
   * When this node is up and running with a valid {@link Peer} representation, the given callback
   * will be invoked.  If the callback is added after this node is ready, it is invoked immediately.
   * @param callback
   */
  void subscribeReady(ReadyCallback callback);

  interface ReadyCallback {
    void onReady(Peer localNode);
  }
}
