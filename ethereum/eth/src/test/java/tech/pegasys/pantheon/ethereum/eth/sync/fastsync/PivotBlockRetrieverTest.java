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
package tech.pegasys.pantheon.ethereum.eth.sync.fastsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.ProtocolContext;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.BlockHeaderTestFixture;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManager;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManagerTestUtil;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer.Responder;
import tech.pegasys.pantheon.ethereum.eth.manager.ethtaskutils.BlockchainSetupUtil;
import tech.pegasys.pantheon.ethereum.mainnet.ProtocolSchedule;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.util.ExceptionUtils;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

public class PivotBlockRetrieverTest {

  private static final long PIVOT_BLOCK_NUMBER = 10;

  private ProtocolContext<Void> protocolContext;

  private final MetricsSystem metricsSystem = new NoOpMetricsSystem();
  private final AtomicBoolean timeout = new AtomicBoolean(false);
  private EthProtocolManager ethProtocolManager;
  private MutableBlockchain blockchain;
  private PivotBlockRetriever<Void> pivotBlockRetriever;
  private ProtocolSchedule<Void> protocolSchedule;

  @Before
  public void setUp() {
    final BlockchainSetupUtil<Void> blockchainSetupUtil = BlockchainSetupUtil.forTesting();
    blockchainSetupUtil.importAllBlocks();
    blockchain = blockchainSetupUtil.getBlockchain();
    protocolSchedule = blockchainSetupUtil.getProtocolSchedule();
    protocolContext = blockchainSetupUtil.getProtocolContext();
    ethProtocolManager =
        EthProtocolManagerTestUtil.create(
            blockchain, blockchainSetupUtil.getWorldArchive(), timeout::get);
    pivotBlockRetriever = createPivotBlockRetriever(3, 1, 1);
  }

  private PivotBlockRetriever<Void> createPivotBlockRetriever(
      final int peersToQuery, final long pivotBlockDelta, final int maxRetries) {
    return pivotBlockRetriever =
        spy(
            new PivotBlockRetriever<>(
                protocolSchedule,
                ethProtocolManager.ethContext(),
                metricsSystem,
                PIVOT_BLOCK_NUMBER,
                peersToQuery,
                pivotBlockDelta,
                maxRetries));
  }

  @Test
  public void shouldSucceedWhenAllPeersAgree() {
    final Responder responder =
        RespondingEthPeer.blockchainResponder(blockchain, protocolContext.getWorldStateArchive());
    final RespondingEthPeer respondingPeerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final RespondingEthPeer respondingPeerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final RespondingEthPeer respondingPeerC =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    final CompletableFuture<FastSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();
    respondingPeerA.respond(responder);
    respondingPeerB.respond(responder);
    respondingPeerC.respond(responder);

    assertThat(future)
        .isCompletedWithValue(
            new FastSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get()));
  }

  @Test
  public void shouldQueryBestPeersFirst() {
    pivotBlockRetriever = createPivotBlockRetriever(2, 1, 1);
    EthProtocolManagerTestUtil.disableEthSchedulerAutoRun(ethProtocolManager);

    final Responder responder =
        RespondingEthPeer.blockchainResponder(blockchain, protocolContext.getWorldStateArchive());

    final RespondingEthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, UInt256.of(1000), 1000);
    final RespondingEthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, UInt256.of(500), 500);
    final RespondingEthPeer peerC =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, UInt256.of(1000), 1000);

    final CompletableFuture<FastSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();

    peerA.respond(responder);
    peerC.respond(responder);

    // Peers A and C should be queried because they have better chain stats
    verify(pivotBlockRetriever, never()).createGetHeaderTask(peerB.getEthPeer());
    assertThat(future)
        .isCompletedWithValue(
            new FastSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get()));
  }

  @Test
  public void shouldIgnorePeersThatDoNotHaveThePivotBlock() {
    pivotBlockRetriever = createPivotBlockRetriever(3, 1, 1);
    EthProtocolManagerTestUtil.disableEthSchedulerAutoRun(ethProtocolManager);

    final Responder responder =
        RespondingEthPeer.blockchainResponder(blockchain, protocolContext.getWorldStateArchive());
    final RespondingEthPeer respondingPeerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final RespondingEthPeer badPeerA = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1);
    final RespondingEthPeer badPeerB = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1);

    final CompletableFuture<FastSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();
    respondingPeerA.respond(responder);

    // With only one peer with sufficient height, we should not be done yet
    assertThat(future).isNotCompleted();

    // Check that invalid peers were not queried
    verify(pivotBlockRetriever, never()).createGetHeaderTask(badPeerA.getEthPeer());
    verify(pivotBlockRetriever, never()).createGetHeaderTask(badPeerB.getEthPeer());

    // Add new peer that we can query
    final RespondingEthPeer respondingPeerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    respondingPeerB.respond(responder);

    // We need one more responsive peer before we're done
    assertThat(future).isNotCompleted();
    verify(pivotBlockRetriever, never()).createGetHeaderTask(badPeerA.getEthPeer());
    verify(pivotBlockRetriever, never()).createGetHeaderTask(badPeerB.getEthPeer());

    // Add new peer that we can query
    final RespondingEthPeer respondingPeerC =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    respondingPeerC.respond(responder);
    verify(pivotBlockRetriever, never()).createGetHeaderTask(badPeerA.getEthPeer());
    verify(pivotBlockRetriever, never()).createGetHeaderTask(badPeerB.getEthPeer());

    assertThat(future)
        .isCompletedWithValue(
            new FastSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get()));
  }

  @Test
  public void shouldRecoverFromUnresponsivePeer() {
    pivotBlockRetriever = createPivotBlockRetriever(2, 1, 1);
    EthProtocolManagerTestUtil.disableEthSchedulerAutoRun(ethProtocolManager);

    final Responder responder =
        RespondingEthPeer.blockchainResponder(blockchain, protocolContext.getWorldStateArchive());
    final Responder emptyResponder = RespondingEthPeer.emptyResponder();

    final RespondingEthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, UInt256.of(1000), 1000);
    final RespondingEthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, UInt256.of(1000), 1000);
    final RespondingEthPeer peerC =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, UInt256.of(500), 500);

    final CompletableFuture<FastSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();
    peerA.respond(responder);
    peerB.respondTimes(emptyResponder, 2);

    // PeerA should have responded, while peerB is being retried, peerC shouldn't have been queried
    // yet
    assertThat(future).isNotCompleted();
    verify(pivotBlockRetriever, never()).createGetHeaderTask(peerC.getEthPeer());

    // After exhausting retries for peerB, we should try peerC
    peerB.respondTimes(emptyResponder, 2);
    peerC.respond(responder);

    assertThat(future)
        .isCompletedWithValue(
            new FastSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get()));
  }

  @Test
  public void shouldRetryWhenPeersDisagreeOnPivot_successfulRetry() {
    final long pivotBlockDelta = 1;
    pivotBlockRetriever = createPivotBlockRetriever(2, pivotBlockDelta, 1);

    final Responder responderA =
        RespondingEthPeer.blockchainResponder(blockchain, protocolContext.getWorldStateArchive());
    final RespondingEthPeer respondingPeerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    // Only return inconsistent block on the first round
    final Responder responderB = responderForFakeBlocks(PIVOT_BLOCK_NUMBER);
    final RespondingEthPeer respondingPeerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    // Execute task and wait for response
    final CompletableFuture<FastSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();
    respondingPeerA.respond(responderA);
    respondingPeerB.respond(responderB);

    // When disagreement is detected, we should push the pivot block back and retry
    final long newPivotBlock = PIVOT_BLOCK_NUMBER - 1;
    assertThat(future).isNotCompleted();
    assertThat(pivotBlockRetriever.pivotBlockNumber).isEqualTo(newPivotBlock);

    // Another round should reach consensus
    respondingPeerA.respond(responderA);
    respondingPeerB.respond(responderB);

    assertThat(future)
        .isCompletedWithValue(new FastSyncState(blockchain.getBlockHeader(newPivotBlock).get()));
  }

  @Test
  public void shouldRetryWhenPeersDisagreeOnPivot_exceedMaxRetries() {
    final long pivotBlockDelta = 1;
    pivotBlockRetriever = createPivotBlockRetriever(2, pivotBlockDelta, 1);

    final Responder responderA =
        RespondingEthPeer.blockchainResponder(blockchain, protocolContext.getWorldStateArchive());
    final RespondingEthPeer respondingPeerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    final Responder responderB =
        responderForFakeBlocks(PIVOT_BLOCK_NUMBER, PIVOT_BLOCK_NUMBER - pivotBlockDelta);
    final RespondingEthPeer respondingPeerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    // Execute task and wait for response
    final CompletableFuture<FastSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();
    respondingPeerA.respond(responderA);
    respondingPeerB.respond(responderB);

    // When disagreement is detected, we should push the pivot block back and retry
    final long newPivotBlock = PIVOT_BLOCK_NUMBER - 1;
    assertThat(future).isNotCompleted();
    assertThat(pivotBlockRetriever.pivotBlockNumber).isEqualTo(newPivotBlock);

    // Another round of invalid responses should lead to failure
    respondingPeerA.respond(responderA);
    respondingPeerB.respond(responderB);

    assertThat(future).isCompletedExceptionally();
    assertThatThrownBy(future::get)
        .hasRootCauseInstanceOf(FastSyncException.class)
        .extracting(e -> ((FastSyncException) ExceptionUtils.rootCause(e)).getError())
        .isEqualTo(FastSyncError.PIVOT_BLOCK_HEADER_MISMATCH);
  }

  @Test
  public void shouldRetryWhenPeersDisagreeOnPivot_pivotInvalidOnRetry() {
    final long pivotBlockDelta = PIVOT_BLOCK_NUMBER + 1;
    pivotBlockRetriever = createPivotBlockRetriever(2, pivotBlockDelta, 1);

    final Responder responderA =
        RespondingEthPeer.blockchainResponder(blockchain, protocolContext.getWorldStateArchive());
    final RespondingEthPeer respondingPeerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    final Responder responderB = responderForFakeBlocks(PIVOT_BLOCK_NUMBER);
    final RespondingEthPeer respondingPeerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    // Execute task and wait for response
    final CompletableFuture<FastSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();
    respondingPeerA.respond(responderA);
    respondingPeerB.respond(responderB);

    assertThat(future).isCompletedExceptionally();
    assertThatThrownBy(future::get)
        .hasRootCauseInstanceOf(FastSyncException.class)
        .extracting(e -> ((FastSyncException) ExceptionUtils.rootCause(e)).getError())
        .isEqualTo(FastSyncError.PIVOT_BLOCK_HEADER_MISMATCH);
  }

  private Responder responderForFakeBlocks(final long... blockNumbers) {
    final Blockchain mockBlockchain = spy(blockchain);
    for (long blockNumber : blockNumbers) {
      when(mockBlockchain.getBlockHeader(blockNumber))
          .thenReturn(
              Optional.of(
                  new BlockHeaderTestFixture()
                      .number(blockNumber)
                      .extraData(BytesValue.of(1))
                      .buildHeader()));
    }

    return RespondingEthPeer.blockchainResponder(mockBlockchain);
  }
}
