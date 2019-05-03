/*
 * Copyright 2019 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package tech.pegasys.pantheon.ethereum.p2p.permissions;

import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.util.Subscribers;

import java.util.Arrays;
import java.util.List;

public abstract class PeerPermissions {
  private final Subscribers<PermissionsUpdateCallback> updateSubscribers = new Subscribers<>();

  public static PeerPermissions noop() {
    return new PeerPermissions() {

      @Override
      public boolean isPermitted(final Peer peer) {
        return true;
      }
    };
  }

  public static PeerPermissions combine(final PeerPermissions... permissions) {
    return new CombinedPeerPermissions(Arrays.asList(permissions));
  }

  public static PeerPermissions combine(final List<PeerPermissions> permissions) {
    return new CombinedPeerPermissions(permissions);
  }

  /**
   * @param peer The {@link Peer} object representing the remote node
   * @return True if we are allowed to communicate with this peer.
   */
  public abstract boolean isPermitted(final Peer peer);

  public void subscribeUpdate(final PermissionsUpdateCallback callback) {
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

    private CombinedPeerPermissions(final List<PeerPermissions> permissions) {
      this.permissions = permissions;
    }

    @Override
    public void subscribeUpdate(final PermissionsUpdateCallback callback) {
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
  }
}
