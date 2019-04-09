package tech.pegasys.pantheon.ethereum.p2p.peers;

import java.util.Arrays;
import java.util.List;
import tech.pegasys.pantheon.util.Subscribers;

public abstract class PeerPermissions {
  private final Subscribers<PermissionsUpdateCallback> updateSubscribers = new Subscribers<>();

  public static PeerPermissions noop() {
    return new PeerPermissions() {

      @Override
      public boolean isPermitted(final Peer peer) {
        return true;
      }

      @Override
      public boolean isReady() {
        return true;
      }
    };
  }

  public static PeerPermissions combine(PeerPermissions... permissions) {
    return new CombinedPeerPermissions(Arrays.asList(permissions));
  }

  public static PeerPermissions combine(List<PeerPermissions> permissions) {
    return new CombinedPeerPermissions(permissions);
  }

  /**
   * @param peer The {@link Peer} object representing the remote node
   * @return True if we are allowed to communicate with this peer.
   */
  public abstract boolean isPermitted(Peer peer);

  /**
   *
   * @return True if peer permissions are ready to validate new peers on the network.
   */
  public abstract boolean isReady();

  public void subscribeUpdate(PermissionsUpdateCallback callback) {
    updateSubscribers.subscribe(callback);
  }

  protected void dispatchUpdate() {
    updateSubscribers.forEach(PermissionsUpdateCallback::onUpdate);
  }

  public interface PermissionsUpdateCallback {
    void onUpdate();
  }

  private static class CombinedPeerPermissions extends PeerPermissions {
    private final List<PeerPermissions> permissions;

    private CombinedPeerPermissions(
      final List<PeerPermissions> permissions) {
      this.permissions = permissions;
    }

    @Override
    public void subscribeUpdate(PermissionsUpdateCallback callback) {
      for (final PeerPermissions permission : permissions) {
        permission.subscribeUpdate(callback);
      }
    }


    @Override
    public boolean isPermitted(final Peer peer) {
      for (PeerPermissions permission : permissions) {
        if (!permission.isPermitted(peer)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean isReady() {
      for (PeerPermissions permission : permissions) {
        if (!permission.isReady()) {
          return false;
        }
      }
      return true;
    }
  }
}
