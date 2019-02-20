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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator.BlockOptions;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManager;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManagerTestUtil;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer.Responder;
import tech.pegasys.pantheon.ethereum.eth.messages.BlockHeadersMessage;
import tech.pegasys.pantheon.ethereum.eth.messages.EthPV62;
import tech.pegasys.pantheon.ethereum.eth.messages.GetBlockHeadersMessage;
import tech.pegasys.pantheon.ethereum.mainnet.MainnetBlockHeaderValidator;
import tech.pegasys.pantheon.ethereum.mainnet.MainnetProtocolSchedule;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import org.junit.Test;

public class DaoForkPeerValidatorTest {

  @Test
  public void validatesNewPeerOnRightSideOfDaoFork_noBuffer() {
    validatesNewPeerOnRightSideOfDaoFork(0);
  }

  @Test
  public void validatesNewPeerOnRightSideOfDaoFork_withBuffer() {
    validatesNewPeerOnRightSideOfDaoFork(10);
  }

  private void validatesNewPeerOnRightSideOfDaoFork(final int chainHeightBuffer) {
    EthProtocolManager ethProtocolManager = EthProtocolManagerTestUtil.create();
    BlockDataGenerator gen = new BlockDataGenerator(1);

    long daoBlockNumber = 500;
    Block daoBlock =
        gen.block(
            BlockOptions.create()
                .setBlockNumber(daoBlockNumber)
                .setExtraData(MainnetBlockHeaderValidator.DAO_EXTRA_DATA));
    PeerValidator daoValidator =
        spy(
            new DaoForkPeerValidator(
                ethProtocolManager.ethContext(),
                MainnetProtocolSchedule.create(),
                NoOpMetricsSystem.NO_OP_LABELLED_TIMER,
                daoBlockNumber,
                0));
    TrackablePeerValidator validator =
        new TrackablePeerValidator(ethProtocolManager.ethContext(), daoValidator);

    // Create new peer that should be validated
    RespondingEthPeer peer =
        spy(
            EthProtocolManagerTestUtil.createPeer(
                ethProtocolManager, daoBlockNumber + chainHeightBuffer));

    // Validator should request the DAO block
    // Setup responder to serve this request
    Responder responder =
        RespondingEthPeer.targetedResponder(
            (cap, msg) -> {
              if (msg.getCode() != EthPV62.GET_BLOCK_HEADERS) {
                return false;
              }
              GetBlockHeadersMessage headersRequest = GetBlockHeadersMessage.readFrom(msg);
              return headersRequest.blockNumber().isPresent()
                  && headersRequest.blockNumber().getAsLong() == daoBlockNumber;
            },
            (cap, msg) -> BlockHeadersMessage.create(daoBlock.getHeader()));

    // Respond
    peer.respond(responder);

    // Verify
    verify(daoValidator, never()).disconnectPeer(any());
    assertThat(validator.hasPeerBeenAccepted(peer.getEthPeer())).isTrue();
  }

  @Test
  public void rejectsPeervOnWrongSideOfDaoFork_noBuffer() {
    rejectsPeerOnWrongSideOfDaoFork(0);
  }

  @Test
  public void rejectsPeervOnWrongSideOfDaoFork_withBuffer() {
    rejectsPeerOnWrongSideOfDaoFork(10);
  }

  private void rejectsPeerOnWrongSideOfDaoFork(final int chainHeightBuffer) {
    EthProtocolManager ethProtocolManager = EthProtocolManagerTestUtil.create();
    BlockDataGenerator gen = new BlockDataGenerator(1);

    long daoBlockNumber = 500;
    Block daoBlock =
        gen.block(
            BlockOptions.create().setBlockNumber(daoBlockNumber).setExtraData(BytesValue.EMPTY));
    PeerValidator daoValidator =
        spy(
            new DaoForkPeerValidator(
                ethProtocolManager.ethContext(),
                MainnetProtocolSchedule.create(),
                NoOpMetricsSystem.NO_OP_LABELLED_TIMER,
                daoBlockNumber,
                0));
    TrackablePeerValidator validator =
        new TrackablePeerValidator(ethProtocolManager.ethContext(), daoValidator);

    // Create new peer that should be validated
    RespondingEthPeer peer =
        EthProtocolManagerTestUtil.createPeer(
            ethProtocolManager, daoBlockNumber + chainHeightBuffer);

    // Validator should request the DAO block
    // Setup responder to serve this request
    Responder responder =
        RespondingEthPeer.targetedResponder(
            (cap, msg) -> {
              if (msg.getCode() != EthPV62.GET_BLOCK_HEADERS) {
                return false;
              }
              GetBlockHeadersMessage headersRequest = GetBlockHeadersMessage.readFrom(msg);
              return headersRequest.blockNumber().isPresent()
                  && headersRequest.blockNumber().getAsLong() == daoBlockNumber;
            },
            (cap, msg) -> BlockHeadersMessage.create(daoBlock.getHeader()));

    // Respond
    peer.respond(responder);

    // Verify
    assertThat(validator.hasPeerBeenRejected(peer.getEthPeer())).isTrue();
    verify(daoValidator, times(1)).disconnectPeer(eq(peer.getEthPeer()));
  }

  @Test
  public void retriesTrailingPeer() {
    EthProtocolManager ethProtocolManager = EthProtocolManagerTestUtil.create();
    EthProtocolManagerTestUtil.disableEthSchedulerAutoRun(ethProtocolManager);
    BlockDataGenerator gen = new BlockDataGenerator(1);

    long daoBlockNumber = 500;
    Block daoBlock =
        gen.block(
            BlockOptions.create().setBlockNumber(daoBlockNumber).setExtraData(BytesValue.EMPTY));
    TrackablePeerValidator validator =
        new TrackablePeerValidator(
            ethProtocolManager.ethContext(),
            new DaoForkPeerValidator(
                ethProtocolManager.ethContext(),
                MainnetProtocolSchedule.create(),
                NoOpMetricsSystem.NO_OP_LABELLED_TIMER,
                daoBlockNumber,
                0));

    // Create new peer that should be validated
    RespondingEthPeer peer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, daoBlockNumber - 100);

    assertThat(validator.peerValidationIsPending(peer.getEthPeer())).isTrue();
    assertThat(validator.getNumberOfChecksForPeer(peer.getEthPeer())).isEqualTo(1);

    // Run pending futures
    final int retries = 2;
    for (int i = 0; i < retries; i++) {
      EthProtocolManagerTestUtil.runPendingFutures(ethProtocolManager);
    }

    assertThat(validator.peerValidationIsPending(peer.getEthPeer())).isTrue();
    assertThat(validator.getNumberOfChecksForPeer(peer.getEthPeer())).isEqualTo(1 + retries);

    // Now update peer stats
    peer.getEthPeer().chainState().update(daoBlock.getHeader());
    // Manually run scheduled tasks to trigger another check
    EthProtocolManagerTestUtil.runPendingFutures(ethProtocolManager);

    // Validator should request the DAO block
    // Setup responder to serve this request
    Responder responder =
        RespondingEthPeer.targetedResponder(
            (cap, msg) -> {
              if (msg.getCode() != EthPV62.GET_BLOCK_HEADERS) {
                return false;
              }
              GetBlockHeadersMessage headersRequest = GetBlockHeadersMessage.readFrom(msg);
              return headersRequest.blockNumber().isPresent()
                  && headersRequest.blockNumber().getAsLong() == daoBlockNumber;
            },
            (cap, msg) -> BlockHeadersMessage.create(daoBlock.getHeader()));

    // Respond
    peer.respond(responder);

    assertThat(validator.peerValidationIsPending(peer.getEthPeer())).isFalse();
    assertThat(validator.hasPeerBeenRejected(peer.getEthPeer())).isTrue();
    assertThat(validator.getNumberOfChecksForPeer(peer.getEthPeer())).isEqualTo(2 + retries);
  }
}
