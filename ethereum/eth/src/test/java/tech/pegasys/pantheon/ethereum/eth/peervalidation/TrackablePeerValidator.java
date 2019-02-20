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
package tech.pegasys.pantheon.ethereum.eth.peervalidation;

import tech.pegasys.pantheon.ethereum.eth.manager.EthContext;
import tech.pegasys.pantheon.ethereum.eth.manager.EthPeer;
import tech.pegasys.pantheon.ethereum.eth.peervalidation.TrackablePeerValidator.PeerValidationStatus.PeerStatus;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TrackablePeerValidator extends PeerValidator {
  private final PeerValidator wrappedValidator;
  private final Map<EthPeer, PeerValidationStatus> trackedPeers = new HashMap<>();

  public TrackablePeerValidator(final EthContext ethContext, final PeerValidator wrappedValidator) {
    super(ethContext);
    this.wrappedValidator = wrappedValidator;
  }

  public int getNumberOfChecksForPeer(final EthPeer peer) {
    PeerValidationStatus trackedPeer = trackedPeers.get(peer);
    if (trackedPeer == null) {
      return 0;
    }
    return trackedPeer.getPeerCheckCount();
  }

  public boolean peerValidationIsPending(final EthPeer peer) {
    PeerValidationStatus trackedPeer = trackedPeers.get(peer);
    if (trackedPeer == null) {
      return false;
    }
    return trackedPeer.getStatus() == PeerStatus.PENDING_VALIDATION;
  }

  public boolean hasPeerBeenAccepted(final EthPeer peer) {
    PeerValidationStatus trackedPeer = trackedPeers.get(peer);
    if (trackedPeer == null) {
      return false;
    }
    return trackedPeer.getStatus() == PeerStatus.VALIDATED;
  }

  public boolean hasPeerBeenRejected(final EthPeer peer) {
    PeerValidationStatus trackedPeer = trackedPeers.get(peer);
    if (trackedPeer == null) {
      return false;
    }
    return trackedPeer.getStatus() == PeerStatus.REJECTED;
  }

  @Override
  void checkPeer(final EthPeer ethPeer) {
    trackedPeers.computeIfAbsent(ethPeer, (k) -> new PeerValidationStatus());
    trackedPeers.get(ethPeer).incrementPeerCheckCount();
    super.checkPeer(ethPeer);
  }

  @Override
  void disconnectPeer(final EthPeer ethPeer) {
    wrappedValidator.disconnectPeer(ethPeer);
  }

  @Override
  protected CompletableFuture<Boolean> validatePeer(final EthPeer ethPeer) {
    CompletableFuture<Boolean> result = wrappedValidator.validatePeer(ethPeer);
    result.whenComplete(
        (valid, err) -> {
          if (err != null) {
            return;
          }
          if (valid) {
            trackedPeers.get(ethPeer).setValidated();
          } else {
            trackedPeers.get(ethPeer).setRejected();
          }
        });
    return result;
  }

  @Override
  protected boolean canBeValidated(final EthPeer ethPeer) {
    return wrappedValidator.canBeValidated(ethPeer);
  }

  @Override
  protected Duration nextCheckTimeout(final EthPeer ethPeer) {
    return wrappedValidator.nextCheckTimeout(ethPeer);
  }

  protected static class PeerValidationStatus {
    public enum PeerStatus {
      PENDING_VALIDATION,
      VALIDATED,
      REJECTED
    }

    private PeerStatus status = PeerStatus.PENDING_VALIDATION;
    private int peerCheckCount = 0;

    public PeerStatus getStatus() {
      return status;
    }

    public void setValidated() {
      this.status = PeerStatus.VALIDATED;
    }

    public void setRejected() {
      this.status = PeerStatus.REJECTED;
    }

    public int getPeerCheckCount() {
      return peerCheckCount;
    }

    public void incrementPeerCheckCount() {
      peerCheckCount++;
    }
  }
}
