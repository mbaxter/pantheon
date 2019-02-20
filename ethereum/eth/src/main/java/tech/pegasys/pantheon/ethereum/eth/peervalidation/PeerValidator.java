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
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;

public abstract class PeerValidator {
  protected final EthContext ethContext;

  protected PeerValidator(final EthContext ethContext) {
    this.ethContext = ethContext;

    ethContext.getEthPeers().subscribeConnect(this::checkPeer);
  }

  void checkPeer(final EthPeer ethPeer) {
    if (canBeValidated(ethPeer)) {
      validatePeer(ethPeer)
          .whenComplete(
              (validated, err) -> {
                if (err != null || !validated) {
                  // Disconnect invalid peer
                  disconnectPeer(ethPeer);
                }
              });
    } else {
      scheduleNextCheck(ethPeer);
    }
  }

  @VisibleForTesting
  void disconnectPeer(final EthPeer ethPeer) {
    ethPeer.disconnect(getDisconnectReason());
  }

  @VisibleForTesting
  void scheduleNextCheck(final EthPeer ethPeer) {
    ethContext
        .getScheduler()
        .scheduleFutureTask(() -> checkPeer(ethPeer), nextCheckTimeout(ethPeer));
  }

  protected DisconnectReason getDisconnectReason() {
    return DisconnectReason.SUBPROTOCOL_TRIGGERED;
  }

  protected abstract CompletableFuture<Boolean> validatePeer(final EthPeer ethPeer);

  protected abstract boolean canBeValidated(final EthPeer ethPeer);

  protected abstract Duration nextCheckTimeout(final EthPeer ethPeer);
}
