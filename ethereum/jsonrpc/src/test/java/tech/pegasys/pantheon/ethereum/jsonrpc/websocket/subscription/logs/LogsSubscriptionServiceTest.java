/*
 * Copyright 2018 ConsenSys AG.
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
package tech.pegasys.pantheon.ethereum.jsonrpc.websocket.subscription.logs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator.BlockOptions;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.InMemoryStorageProvider;
import tech.pegasys.pantheon.ethereum.core.Log;
import tech.pegasys.pantheon.ethereum.core.LogTopic;
import tech.pegasys.pantheon.ethereum.core.Transaction;
import tech.pegasys.pantheon.ethereum.core.TransactionReceipt;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.FilterParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.TopicsParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.queries.BlockchainQueries;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.LogResult;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.Quantity;
import tech.pegasys.pantheon.ethereum.jsonrpc.websocket.subscription.SubscriptionManager;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateArchive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LogsSubscriptionServiceTest {

  private final BlockDataGenerator gen = new BlockDataGenerator(1);
  private final WorldStateArchive worldStateArchive =
      InMemoryStorageProvider.createInMemoryWorldStateArchive();
  private final MutableBlockchain blockchain =
      InMemoryStorageProvider.createInMemoryBlockchain(gen.genesisBlock());
  private BlockchainQueries blockchainQueries =
      new BlockchainQueries(blockchain, worldStateArchive);

  private LogsSubscriptionService logsSubscriptionService;
  private final AtomicLong nextSubscriptionId = new AtomicLong();

  @Mock private SubscriptionManager subscriptionManager;

  @Before
  public void before() {
    logsSubscriptionService = new LogsSubscriptionService(subscriptionManager, blockchainQueries);
    blockchain.observeBlockAdded(logsSubscriptionService);
  }

  @Test
  public void singleMatchingLogEvent() {
    final BlockWithReceipts blockWithReceipts = generateBlock(2, 2, 2);
    final Block block = blockWithReceipts.getBlock();
    final List<TransactionReceipt> receipts = blockWithReceipts.getReceipts();

    final int txIndex = 1;
    final int logIndex = 1;
    final Log targetLog = receipts.get(txIndex).getLogs().get(logIndex);

    final LogsSubscription subscription = createSubscription(targetLog.getLogger());
    registerSubscriptions(subscription);
    blockchain.appendBlock(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts());

    final ArgumentCaptor<LogResult> captor = ArgumentCaptor.forClass(LogResult.class);
    verify(subscriptionManager).sendMessage(eq(subscription.getSubscriptionId()), captor.capture());

    final List<LogResult> logResults = captor.getAllValues();

    assertThat(logResults.size()).isEqualTo(1);
    final LogResult result = logResults.get(0);
    assertLogResultMatches(result, block, receipts, txIndex, logIndex, false);
  }

  @Test
  public void singleMatchingLogEmittedThenRemovedInReorg() {
    // Create block that emits an event
    final BlockWithReceipts blockWithReceipts = generateBlock(2, 2, 2);
    final Block block = blockWithReceipts.getBlock();
    final List<TransactionReceipt> receipts = blockWithReceipts.getReceipts();

    final int txIndex = 1;
    final int logIndex = 1;
    final Log targetLog = receipts.get(txIndex).getLogs().get(logIndex);

    final LogsSubscription subscription = createSubscription(targetLog.getLogger());
    registerSubscriptions(subscription);
    blockchain.appendBlock(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts());

    // Cause a reorg that removes the block which emitted an event
    BlockHeader parentHeader = blockchain.getGenesisBlock().getHeader();
    while (!blockchain.getChainHeadHash().equals(parentHeader.getHash())) {
      final BlockWithReceipts newBlock = generateBlock(parentHeader, 2, 0, 0);
      parentHeader = newBlock.getBlock().getHeader();
      blockchain.appendBlock(newBlock.getBlock(), newBlock.getReceipts());
    }

    final ArgumentCaptor<LogResult> captor = ArgumentCaptor.forClass(LogResult.class);
    verify(subscriptionManager, times(2))
        .sendMessage(eq(subscription.getSubscriptionId()), captor.capture());

    final List<LogResult> logResults = captor.getAllValues();

    assertThat(logResults.size()).isEqualTo(2);
    final LogResult firstLog = logResults.get(0);
    assertLogResultMatches(firstLog, block, receipts, txIndex, logIndex, false);
    final LogResult secondLog = logResults.get(1);
    assertLogResultMatches(secondLog, block, receipts, txIndex, logIndex, true);
  }

  @Test
  public void singleMatchingLogEmittedThenMovedInReorg() {
    // Create block that emits an event
    final BlockWithReceipts blockWithReceipts = generateBlock(2, 2, 2);
    final Block block = blockWithReceipts.getBlock();
    final List<TransactionReceipt> receipts = blockWithReceipts.getReceipts();

    final int txIndex = 1;
    final int logIndex = 1;
    final Log targetLog = receipts.get(txIndex).getLogs().get(logIndex);

    final LogsSubscription subscription = createSubscription(targetLog.getLogger());
    registerSubscriptions(subscription);
    blockchain.appendBlock(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts());

    // Cause a reorg that removes the block which emitted an event
    BlockHeader parentHeader = blockchain.getGenesisBlock().getHeader();
    while (!blockchain.getChainHeadHash().equals(parentHeader.getHash())) {
      final BlockWithReceipts newBlock = generateBlock(parentHeader, 2, 0, 0);
      parentHeader = newBlock.getBlock().getHeader();
      blockchain.appendBlock(newBlock.getBlock(), newBlock.getReceipts());
    }

    // Now add another block that re-emits the target log
    final BlockWithReceipts newBlockWithLog = generateBlock(1, () -> Arrays.asList(targetLog));
    blockchain.appendBlock(newBlockWithLog.getBlock(), newBlockWithLog.getReceipts());
    // Sanity check
    assertThat(blockchain.getChainHeadHash()).isEqualTo(newBlockWithLog.getBlock().getHash());

    final ArgumentCaptor<LogResult> captor = ArgumentCaptor.forClass(LogResult.class);
    verify(subscriptionManager, times(3))
        .sendMessage(eq(subscription.getSubscriptionId()), captor.capture());

    final List<LogResult> logResults = captor.getAllValues();

    assertThat(logResults.size()).isEqualTo(3);
    final LogResult originalLog = logResults.get(0);
    assertLogResultMatches(originalLog, block, receipts, txIndex, logIndex, false);
    final LogResult removedLog = logResults.get(1);
    assertLogResultMatches(removedLog, block, receipts, txIndex, logIndex, true);
    final LogResult updatedLog = logResults.get(1);
    assertLogResultMatches(
        updatedLog, newBlockWithLog.getBlock(), newBlockWithLog.getReceipts(), 0, 0, false);
  }

  @Test
  public void multipleMatchingLogsEmitted() {
    final Log targetLog = gen.log();
    final Log otherLog = gen.log();
    final List<Log> logs = Arrays.asList(targetLog, otherLog);

    final LogsSubscription subscription = createSubscription(targetLog.getLogger());
    registerSubscriptions(subscription);

    // Generate blocks with multiple logs matching subscription
    final int txCount = 2;
    final List<BlockWithReceipts> targetBlocks = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      final BlockWithReceipts blockWithReceipts = generateBlock(txCount, () -> logs);
      targetBlocks.add(blockWithReceipts);
      blockchain.appendBlock(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts());

      // Add another block with unrelated logs
      final BlockWithReceipts otherBlock = generateBlock(txCount, 2, 2);
      blockchain.appendBlock(otherBlock.getBlock(), otherBlock.getReceipts());
    }

    final ArgumentCaptor<LogResult> captor = ArgumentCaptor.forClass(LogResult.class);
    verify(subscriptionManager, times(targetBlocks.size() * txCount))
        .sendMessage(eq(subscription.getSubscriptionId()), captor.capture());
    final List<LogResult> logResults = captor.getAllValues();

    // Verify all logs are emitted
    assertThat(logResults.size()).isEqualTo(targetBlocks.size() * txCount);
    for (int i = 0; i < targetBlocks.size(); i++) {
      final BlockWithReceipts targetBlock = targetBlocks.get(i);
      for (int j = 0; j < txCount; j++) {
        final int resultIndex = i * txCount + j;
        assertLogResultMatches(
            logResults.get(resultIndex),
            targetBlock.getBlock(),
            targetBlock.getReceipts(),
            j,
            0,
            false);
      }
    }
  }

  @Test
  public void multipleSubscriptionsForSingleMatchingLog() {
    final BlockWithReceipts blockWithReceipts = generateBlock(2, 2, 2);
    final Block block = blockWithReceipts.getBlock();
    final List<TransactionReceipt> receipts = blockWithReceipts.getReceipts();

    final int txIndex = 1;
    final int logIndex = 1;
    final Log targetLog = receipts.get(txIndex).getLogs().get(logIndex);

    final List<LogsSubscription> subscriptions =
        Stream.generate(() -> createSubscription(targetLog.getLogger()))
            .limit(3)
            .collect(Collectors.toList());
    registerSubscriptions(subscriptions);
    blockchain.appendBlock(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts());

    for (LogsSubscription subscription : subscriptions) {
      final ArgumentCaptor<LogResult> captor = ArgumentCaptor.forClass(LogResult.class);
      verify(subscriptionManager)
          .sendMessage(eq(subscription.getSubscriptionId()), captor.capture());

      final List<LogResult> logResults = captor.getAllValues();

      assertThat(logResults.size()).isEqualTo(1);
      final LogResult result = logResults.get(0);
      assertLogResultMatches(result, block, receipts, txIndex, logIndex, false);
    }
  }

  @Test
  public void noLogsEmitted() {
    final Address address = Address.fromHexString("0x0");
    final LogsSubscription subscription = createSubscription(address);
    registerSubscriptions(subscription);

    final BlockWithReceipts blockWithReceipts = generateBlock(2, 0, 0);
    blockchain.appendBlock(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts());

    final ArgumentCaptor<LogResult> captor = ArgumentCaptor.forClass(LogResult.class);
    verify(subscriptionManager, times(0))
        .sendMessage(eq(subscription.getSubscriptionId()), captor.capture());
  }

  @Test
  public void noMatchingLogsEmitted() {
    final Address address = Address.fromHexString("0x0");
    final LogsSubscription subscription = createSubscription(address);
    registerSubscriptions(subscription);

    final BlockWithReceipts blockWithReceipts = generateBlock(2, 2, 2);
    blockchain.appendBlock(blockWithReceipts.getBlock(), blockWithReceipts.getReceipts());

    final ArgumentCaptor<LogResult> captor = ArgumentCaptor.forClass(LogResult.class);
    verify(subscriptionManager, times(0))
        .sendMessage(eq(subscription.getSubscriptionId()), captor.capture());
  }

  private void assertLogResultMatches(
      final LogResult result,
      final Block block,
      final List<TransactionReceipt> receipts,
      final int txIndex,
      final int logIndex,
      final boolean isRemoved) {
    final Transaction expectedTransaction = block.getBody().getTransactions().get(txIndex);
    final Log expectedLog = receipts.get(txIndex).getLogs().get(logIndex);

    assertThat(result.getLogIndex()).isEqualTo(Quantity.create(logIndex));
    assertThat(result.getTransactionIndex()).isEqualTo(Quantity.create(txIndex));
    assertThat(result.getBlockNumber()).isEqualTo(Quantity.create(block.getHeader().getNumber()));
    assertThat(result.getBlockHash()).isEqualTo(block.getHash().toString());
    assertThat(result.getTransactionHash()).isEqualTo(expectedTransaction.hash().toString());
    assertThat(result.getAddress()).isEqualTo(expectedLog.getLogger().toString());
    assertThat(result.getData()).isEqualTo(expectedLog.getData().toString());
    assertThat(result.getTopics())
        .isEqualTo(
            expectedLog.getTopics().stream().map(LogTopic::toString).collect(Collectors.toList()));
    assertThat(result.isRemoved()).isEqualTo(isRemoved);
  }

  private BlockWithReceipts generateBlock(
      final int txCount, final int logsPerTx, final int topicsPerLog) {
    final BlockHeader parent = blockchain.getChainHeadHeader();
    return generateBlock(parent, txCount, () -> gen.logs(logsPerTx, topicsPerLog));
  }

  private BlockWithReceipts generateBlock(
      final BlockHeader parentHeader,
      final int txCount,
      final int logsPerTx,
      final int topicsPerLog) {
    return generateBlock(parentHeader, txCount, () -> gen.logs(logsPerTx, topicsPerLog));
  }

  private BlockWithReceipts generateBlock(
      final int txCount, final Supplier<List<Log>> logsSupplier) {
    final BlockHeader parent = blockchain.getChainHeadHeader();
    return generateBlock(parent, txCount, logsSupplier);
  }

  private BlockWithReceipts generateBlock(
      final BlockHeader parentHeader, final int txCount, final Supplier<List<Log>> logsSupplier) {
    final List<TransactionReceipt> receipts = new ArrayList<>();
    final List<Log> logs = new ArrayList<>();
    final BlockOptions blockOptions = BlockOptions.create();
    for (int i = 0; i < txCount; i++) {
      final Transaction tx = gen.transaction();
      final TransactionReceipt receipt = gen.receipt(logsSupplier.get());

      receipts.add(receipt);
      receipt.getLogs().forEach(logs::add);
      blockOptions.addTransaction(tx);
    }

    blockOptions.setParentHash(parentHeader.getHash());
    blockOptions.setBlockNumber(parentHeader.getNumber() + 1L);
    final Block block = gen.block(blockOptions);

    return new BlockWithReceipts(block, receipts);
  }

  private LogsSubscription createSubscription(final Address address) {
    return createSubscription(Arrays.asList(address), Collections.emptyList());
  }

  private LogsSubscription createSubscription(
      final List<Address> addresses, final List<List<LogTopic>> logTopics) {
    // TODO: FilterParameter constructor should work with proper types instead of Strings
    final List<String> addressStrings =
        addresses.stream().map(Address::toString).collect(Collectors.toList());
    final List<List<String>> topicStrings =
        logTopics.stream()
            .map(topics -> topics.stream().map(LogTopic::toString).collect(Collectors.toList()))
            .collect(Collectors.toList());

    final FilterParameter filterParameter =
        new FilterParameter(null, null, addressStrings, new TopicsParameter(topicStrings), null);
    final LogsSubscription logsSubscription =
        new LogsSubscription(nextSubscriptionId.incrementAndGet(), "conn", filterParameter);
    return logsSubscription;
  }

  private void registerSubscriptions(final LogsSubscription... subscriptions) {
    registerSubscriptions(Arrays.asList(subscriptions));
  }

  private void registerSubscriptions(final List<LogsSubscription> subscriptions) {
    when(subscriptionManager.subscriptionsOfType(any(), any()))
        .thenReturn(Lists.newArrayList(subscriptions));
  }

  private static class BlockWithReceipts {
    private final Block block;
    private final List<TransactionReceipt> receipts;

    public BlockWithReceipts(final Block block, final List<TransactionReceipt> receipts) {
      this.block = block;
      this.receipts = receipts;
    }

    public Block getBlock() {
      return block;
    }

    public List<TransactionReceipt> getReceipts() {
      return receipts;
    }
  }
}
