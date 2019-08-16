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
package tech.pegasys.pantheon.ethereum.eth.sync;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static tech.pegasys.pantheon.ethereum.eth.sync.PipelineChainDownloader.PAUSE_AFTER_ERROR_DURATION;

import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.BlockHeaderTestFixture;
import tech.pegasys.pantheon.ethereum.eth.manager.EthPeer;
import tech.pegasys.pantheon.ethereum.eth.manager.EthScheduler;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncState;
import tech.pegasys.pantheon.ethereum.eth.sync.state.SyncTarget;
import tech.pegasys.pantheon.ethereum.eth.sync.tasks.exceptions.InvalidBlockException;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.services.pipeline.Pipeline;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PipelineChainDownloaderTest {

  @Mock private SyncTargetManager<Void> syncTargetManager;
  @Mock private DownloadPipelineFactory downloadPipelineFactory;
  @Mock private EthScheduler scheduler;
  @Mock private Pipeline<?> downloadPipeline;
  @Mock private Pipeline<Object> downloadPipeline2;
  @Mock private EthPeer peer1;
  @Mock private EthPeer peer2;
  @Mock private SyncState syncState;
  private final BlockHeader commonAncestor = new BlockHeaderTestFixture().buildHeader();
  private SyncTarget syncTarget;
  private PipelineChainDownloader<Void> chainDownloader;

  @Before
  public void setUp() {
    syncTarget = new SyncTarget(peer1, commonAncestor);
    chainDownloader =
        new PipelineChainDownloader<>(
            syncState,
            syncTargetManager,
            downloadPipelineFactory,
            scheduler,
            new NoOpMetricsSystem());

    immediatelyCompletePauseAfterError();
  }

  @Test
  public void shouldSelectSyncTargetWhenStarted() {
    when(syncTargetManager.findSyncTarget(Optional.empty())).thenReturn(new CompletableFuture<>());
    chainDownloader.start();

    verify(syncTargetManager).findSyncTarget(Optional.empty());
  }

  @Test
  public void shouldStartChainDownloadWhenTargetSelected() {
    final CompletableFuture<SyncTarget> selectTargetFuture = new CompletableFuture<>();
    when(syncTargetManager.findSyncTarget(Optional.empty())).thenReturn(selectTargetFuture);
    expectPipelineCreation(syncTarget, downloadPipeline);
    when(scheduler.startPipeline(downloadPipeline)).thenReturn(new CompletableFuture<>());
    chainDownloader.start();
    verifyZeroInteractions(downloadPipelineFactory);

    selectTargetFuture.complete(syncTarget);

    verify(downloadPipelineFactory).createDownloadPipelineForSyncTarget(syncTarget);
    verify(scheduler).startPipeline(downloadPipeline);
  }

  @Test
  public void shouldUpdateSyncStateWhenTargetSelected() {
    final CompletableFuture<SyncTarget> selectTargetFuture = new CompletableFuture<>();
    when(syncTargetManager.findSyncTarget(Optional.empty())).thenReturn(selectTargetFuture);
    expectPipelineCreation(syncTarget, downloadPipeline);
    when(scheduler.startPipeline(downloadPipeline)).thenReturn(new CompletableFuture<>());
    chainDownloader.start();
    verifyZeroInteractions(downloadPipelineFactory);

    selectTargetFuture.complete(syncTarget);

    verify(syncState).setSyncTarget(peer1, commonAncestor);
  }

  @Test
  public void shouldRetryWhenSyncTargetSelectionFailsAndSyncTargetManagerShouldContinue() {
    final CompletableFuture<SyncTarget> selectTargetFuture = new CompletableFuture<>();
    when(syncTargetManager.shouldContinueDownloading()).thenReturn(true);
    when(syncTargetManager.findSyncTarget(Optional.empty()))
        .thenReturn(selectTargetFuture)
        .thenReturn(new CompletableFuture<>());
    chainDownloader.start();

    verify(syncTargetManager).findSyncTarget(Optional.empty());

    selectTargetFuture.completeExceptionally(new RuntimeException("Nope"));

    verify(syncTargetManager, times(2)).findSyncTarget(Optional.empty());
  }

  @Test
  public void shouldBeCompleteWhenSyncTargetSelectionFailsAndSyncTargetManagerShouldNotContinue() {
    final CompletableFuture<SyncTarget> selectTargetFuture = new CompletableFuture<>();
    when(syncTargetManager.shouldContinueDownloading()).thenReturn(false);
    when(syncTargetManager.findSyncTarget(Optional.empty()))
        .thenReturn(selectTargetFuture)
        .thenReturn(new CompletableFuture<>());
    final CompletableFuture<Void> result = chainDownloader.start();

    verify(syncTargetManager).findSyncTarget(Optional.empty());

    final RuntimeException exception = new RuntimeException("Nope");
    selectTargetFuture.completeExceptionally(exception);

    assertExceptionallyCompletedWith(result, exception);
  }

  @Test
  public void shouldBeCompleteWhenPipelineCompletesAndSyncTargetManagerShouldNotContinue() {
    final CompletableFuture<Void> pipelineFuture = expectPipelineStarted(syncTarget);
    final CompletableFuture<Void> result = chainDownloader.start();

    verify(syncTargetManager).findSyncTarget(Optional.empty());

    when(syncTargetManager.shouldContinueDownloading()).thenReturn(false);
    pipelineFuture.complete(null);

    verify(syncTargetManager).shouldContinueDownloading();
    verify(syncState).clearSyncTarget();
    verifyNoMoreInteractions(syncTargetManager);
    assertThat(result).isCompleted();
  }

  @Test
  public void shouldSelectNewSyncTargetWhenPipelineCompletesIfSyncTargetManagerShouldContinue() {
    when(syncTargetManager.shouldContinueDownloading()).thenReturn(true);
    final CompletableFuture<Void> pipelineFuture = expectPipelineStarted(syncTarget);

    final CompletableFuture<Void> result = chainDownloader.start();

    final SyncTarget syncTarget2 = new SyncTarget(peer2, commonAncestor);
    verify(syncTargetManager).findSyncTarget(Optional.empty());

    // Setup expectation for second time round.
    expectPipelineStarted(syncTarget2, downloadPipeline2);

    pipelineFuture.complete(null);
    assertThat(result).isNotDone();

    verify(syncTargetManager, times(2)).findSyncTarget(Optional.empty());
    assertThat(result).isNotDone();
  }

  @Test
  public void shouldNotNestExceptionHandling() {
    when(syncTargetManager.shouldContinueDownloading())
        .thenReturn(true) // Allow continuing after first successful download
        .thenReturn(false); // But not after finding the second sync target fails

    final CompletableFuture<SyncTarget> findSecondSyncTargetFuture = new CompletableFuture<>();
    final CompletableFuture<Void> pipelineFuture = expectPipelineStarted(syncTarget);

    final CompletableFuture<Void> result = chainDownloader.start();

    verify(syncTargetManager).findSyncTarget(Optional.empty());

    // Setup expectation for second call
    when(syncTargetManager.findSyncTarget(Optional.empty())).thenReturn(findSecondSyncTargetFuture);

    pipelineFuture.complete(null);
    assertThat(result).isNotDone();

    verify(syncTargetManager, times(2)).findSyncTarget(Optional.empty());
    assertThat(result).isNotDone();

    final RuntimeException exception = new RuntimeException("Nope");
    findSecondSyncTargetFuture.completeExceptionally(exception);

    assertExceptionallyCompletedWith(result, exception);
    // Should only need to check if it should continue twice.
    // We'll wind up doing this check more than necessary if we keep wrapping additional exception
    // handlers when restarting the sequence which wastes memory.
    verify(syncTargetManager, times(2)).shouldContinueDownloading();
  }

  @Test
  public void shouldNotStartDownloadIfCancelledWhileSelectingSyncTarget() {
    final CompletableFuture<SyncTarget> selectSyncTargetFuture = new CompletableFuture<>();
    lenient().when(syncTargetManager.shouldContinueDownloading()).thenReturn(true);
    when(syncTargetManager.findSyncTarget(Optional.empty())).thenReturn(selectSyncTargetFuture);

    final CompletableFuture<Void> result = chainDownloader.start();
    verify(syncTargetManager).findSyncTarget(Optional.empty());

    chainDownloader.cancel();
    // Note the future doesn't complete until all activity has come to a stop.
    assertThat(result).isNotDone();

    selectSyncTargetFuture.complete(syncTarget);
    verifyZeroInteractions(downloadPipelineFactory);
    assertCancelled(result);
  }

  @Test
  public void shouldAbortPipelineIfCancelledAfterDownloadStarts() {
    lenient().when(syncTargetManager.shouldContinueDownloading()).thenReturn(true);
    final CompletableFuture<Void> pipelineFuture = expectPipelineStarted(syncTarget);

    final CompletableFuture<Void> result = chainDownloader.start();
    verify(syncTargetManager).findSyncTarget(Optional.empty());
    verify(downloadPipelineFactory).createDownloadPipelineForSyncTarget(syncTarget);

    chainDownloader.cancel();
    // Pipeline is aborted immediately.
    verify(downloadPipeline).abort();
    // The future doesn't complete until all activity has come to a stop.
    assertThat(result).isNotDone();

    // And then things are complete when the pipeline is actually complete.
    pipelineFuture.completeExceptionally(new CancellationException("Pipeline aborted"));
    assertCancelled(result);
  }

  @Test
  public void shouldDisconnectSyncTargetOnInvalidBlockException_finishedDownloading_cancelled() {
    testInvalidBlockHandling(true, true);
  }

  @Test
  public void
      shouldDisconnectSyncTargetOnInvalidBlockException_notFinishedDownloading_notCancelled() {
    testInvalidBlockHandling(false, false);
  }

  @Test
  public void shouldDisconnectSyncTargetOnInvalidBlockException_finishedDownloading_notCancelled() {
    testInvalidBlockHandling(true, false);
  }

  @Test
  public void shouldDisconnectSyncTargetOnInvalidBlockException_notFinishedDownloading_cancelled() {
    testInvalidBlockHandling(false, true);
  }

  public void testInvalidBlockHandling(
      final boolean isFinishedDownloading, final boolean isCancelled) {
    final CompletableFuture<SyncTarget> selectTargetFuture = new CompletableFuture<>();
    when(syncTargetManager.shouldContinueDownloading()).thenReturn(isFinishedDownloading);
    when(syncTargetManager.findSyncTarget(Optional.empty()))
        .thenReturn(selectTargetFuture)
        .thenReturn(new CompletableFuture<>());
    final EthPeer ethPeer = Mockito.mock(EthPeer.class);
    final BlockHeader commonAncestor = Mockito.mock(BlockHeader.class);
    final SyncTarget target = new SyncTarget(ethPeer, commonAncestor);
    when(syncState.syncTarget()).thenReturn(Optional.of(target));
    chainDownloader.start();
    verify(syncTargetManager).findSyncTarget(Optional.empty());
    if (isCancelled) {
      chainDownloader.cancel();
    }
    selectTargetFuture.completeExceptionally(new InvalidBlockException("", 1, null));
    verify(ethPeer).disconnect(DisconnectMessage.DisconnectReason.BREACH_OF_PROTOCOL);
  }

  private CompletableFuture<Void> expectPipelineStarted(final SyncTarget syncTarget) {
    return expectPipelineStarted(syncTarget, downloadPipeline);
  }

  private CompletableFuture<Void> expectPipelineStarted(
      final SyncTarget syncTarget, final Pipeline<?> pipeline) {
    final CompletableFuture<Void> pipelineFuture = new CompletableFuture<>();
    when(syncTargetManager.findSyncTarget(Optional.empty()))
        .thenReturn(completedFuture(syncTarget));
    expectPipelineCreation(syncTarget, pipeline);
    when(scheduler.startPipeline(pipeline)).thenReturn(pipelineFuture);
    return pipelineFuture;
  }

  @SuppressWarnings("unchecked") // Mockito really doesn't like Pipeline<?>
  private void expectPipelineCreation(final SyncTarget syncTarget, final Pipeline<?> pipeline) {
    when(downloadPipelineFactory.createDownloadPipelineForSyncTarget(syncTarget))
        .thenReturn((Pipeline) pipeline);
  }

  private void assertExceptionallyCompletedWith(
      final CompletableFuture<?> future, final Throwable error) {
    assertThat(future).isCompletedExceptionally();
    assertThatThrownBy(future::get).isInstanceOf(ExecutionException.class).hasRootCause(error);
  }

  private void assertCancelled(final CompletableFuture<?> future) {
    assertThat(future).isCompletedExceptionally();
    assertThatThrownBy(future::get)
        .isInstanceOf(ExecutionException.class)
        .hasRootCauseInstanceOf(CancellationException.class);
  }

  @SuppressWarnings("unchecked")
  private void immediatelyCompletePauseAfterError() {
    when(scheduler.scheduleFutureTask(any(Supplier.class), same(PAUSE_AFTER_ERROR_DURATION)))
        .then(invocation -> ((Supplier<CompletableFuture<?>>) invocation.getArgument(0)).get());
  }
}
