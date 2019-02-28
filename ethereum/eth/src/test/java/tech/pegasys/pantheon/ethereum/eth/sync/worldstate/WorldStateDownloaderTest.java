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
package tech.pegasys.pantheon.ethereum.eth.sync.worldstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.core.Account;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator.BlockOptions;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.BlockHeaderTestFixture;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.MutableWorldState;
import tech.pegasys.pantheon.ethereum.core.WorldState;
import tech.pegasys.pantheon.ethereum.eth.manager.DeterministicEthScheduler;
import tech.pegasys.pantheon.ethereum.eth.manager.DeterministicEthScheduler.TimeoutPolicy;
import tech.pegasys.pantheon.ethereum.eth.manager.EthContext;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManager;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManagerTestUtil;
import tech.pegasys.pantheon.ethereum.eth.manager.EthScheduler;
import tech.pegasys.pantheon.ethereum.eth.manager.MockExecutorService;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer.Responder;
import tech.pegasys.pantheon.ethereum.eth.messages.EthPV63;
import tech.pegasys.pantheon.ethereum.eth.messages.GetNodeDataMessage;
import tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration;
import tech.pegasys.pantheon.ethereum.mainnet.MainnetProtocolSchedule;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.ethereum.storage.keyvalue.KeyValueStorageWorldStateStorage;
import tech.pegasys.pantheon.ethereum.trie.MerklePatriciaTrie;
import tech.pegasys.pantheon.ethereum.trie.Node;
import tech.pegasys.pantheon.ethereum.trie.StoredMerklePatriciaTrie;
import tech.pegasys.pantheon.ethereum.trie.TrieNodeDecoder;
import tech.pegasys.pantheon.ethereum.worldstate.StateTrieAccountValue;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateArchive;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage.Updater;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.services.kvstore.InMemoryKeyValueStorage;
import tech.pegasys.pantheon.services.queue.InMemoryTaskQueue;
import tech.pegasys.pantheon.services.queue.TaskQueue;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Test;

public class WorldStateDownloaderTest {

  private static final Hash EMPTY_TRIE_ROOT = Hash.wrap(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH);

  private final BlockDataGenerator dataGen = new BlockDataGenerator(1);
  private final ExecutorService persistenceThread =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat(WorldStateDownloaderTest.class.getSimpleName() + "-persistence-%d")
              .build());

  @After
  public void tearDown() throws Exception {
    persistenceThread.shutdownNow();
    assertThat(persistenceThread.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void downloadWorldStateFromPeers_onePeerOneWithManyRequestsOneAtATime() {
    downloadAvailableWorldStateFromPeers(1, 50, 1, 1);
  }

  @Test
  public void downloadWorldStateFromPeers_onePeerOneWithManyRequests() {
    downloadAvailableWorldStateFromPeers(1, 50, 1, 10);
  }

  @Test
  public void downloadWorldStateFromPeers_onePeerWithSingleRequest() {
    downloadAvailableWorldStateFromPeers(1, 1, 100, 10);
  }

  @Test
  public void downloadWorldStateFromPeers_largeStateFromMultiplePeers() {
    downloadAvailableWorldStateFromPeers(5, 100, 10, 10);
  }

  @Test
  public void downloadWorldStateFromPeers_smallStateFromMultiplePeers() {
    downloadAvailableWorldStateFromPeers(5, 5, 1, 10);
  }

  @Test
  public void downloadWorldStateFromPeers_singleRequestWithMultiplePeers() {
    downloadAvailableWorldStateFromPeers(5, 1, 50, 50);
  }

  @Test
  public void downloadEmptyWorldState() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));
    final BlockHeader header =
        dataGen
            .block(BlockOptions.create().setStateRoot(EMPTY_TRIE_ROOT).setBlockNumber(10))
            .getHeader();

    // Create some peers
    final List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(5)
            .collect(Collectors.toList());

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateDownloader downloader =
        createDownloader(ethProtocolManager.ethContext(), localStorage, queue);

    final CompletableFuture<Void> future = downloader.run(header);
    assertThat(future).isDone();

    // Peers should not have been queried
    for (final RespondingEthPeer peer : peers) {
      assertThat(peer.hasOutstandingRequests()).isFalse();
    }
  }

  @Test
  public void downloadAlreadyAvailableWorldState() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));

    // Setup existing state
    final WorldStateStorage storage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive worldStateArchive = new WorldStateArchive(storage);
    final MutableWorldState worldState = worldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    dataGen.createRandomAccounts(worldState, 20);
    final Hash stateRoot = worldState.rootHash();
    assertThat(stateRoot).isNotEqualTo(EMPTY_TRIE_ROOT); // Sanity check
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Create some peers
    final List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(5)
            .collect(Collectors.toList());

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateDownloader downloader =
        createDownloader(ethProtocolManager.ethContext(), storage, queue);

    final CompletableFuture<Void> future = downloader.run(header);
    assertThat(future).isDone();

    // Peers should not have been queried because we already had the state
    for (final RespondingEthPeer peer : peers) {
      assertThat(peer.hasOutstandingRequests()).isFalse();
    }
  }

  @Test
  public void canRecoverFromTimeouts() {
    final TimeoutPolicy timeoutPolicy = TimeoutPolicy.timeoutXTimes(2);
    final EthProtocolManager ethProtocolManager = EthProtocolManagerTestUtil.create(timeoutPolicy);
    final MockExecutorService serviceExecutor =
        ((DeterministicEthScheduler) ethProtocolManager.ethContext().getScheduler())
            .mockServiceExecutor();
    serviceExecutor.setAutoRun(false);

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    final List<Account> accounts = dataGen.createRandomAccounts(remoteWorldState, 20);
    final Hash stateRoot = remoteWorldState.rootHash();
    assertThat(stateRoot).isNotEqualTo(EMPTY_TRIE_ROOT); // Sanity check
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Create some peers
    final List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(5)
            .collect(Collectors.toList());

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateDownloader downloader =
        createDownloader(ethProtocolManager.ethContext(), localStorage, queue);

    final CompletableFuture<Void> result = downloader.run(header);

    persistenceThread.submit(serviceExecutor::runPendingFutures);

    // Respond to node data requests
    final Responder responder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);

    while (!result.isDone()) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
      giveOtherThreadsAGo();
    }

    // Check that all expected account data was downloaded
    final WorldStateArchive localWorldStateArchive = new WorldStateArchive(localStorage);
    final WorldState localWorldState = localWorldStateArchive.get(stateRoot).get();
    assertThat(result).isDone();
    assertAccountsMatch(localWorldState, accounts);
  }

  @Test
  public void handlesPartialResponsesFromNetwork() {
    downloadAvailableWorldStateFromPeers(5, 100, 10, 10, this::respondPartially);
  }

  @Test
  public void doesNotRequestKnownCodeFromNetwork() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    final List<Account> accounts =
        dataGen.createRandomContractAccountsWithNonEmptyStorage(remoteWorldState, 20);
    final Hash stateRoot = remoteWorldState.rootHash();
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Create some peers
    final List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(5)
            .collect(Collectors.toList());

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());

    // Seed local storage with some contract values
    final Map<Bytes32, BytesValue> knownCode = new HashMap<>();
    accounts.subList(0, 5).forEach(a -> knownCode.put(a.getCodeHash(), a.getCode()));
    final Updater localStorageUpdater = localStorage.updater();
    knownCode.forEach(localStorageUpdater::putCode);
    localStorageUpdater.commit();

    final WorldStateDownloader downloader =
        createDownloader(ethProtocolManager.ethContext(), localStorage, queue);

    final CompletableFuture<Void> result = downloader.run(header);

    // Respond to node data requests
    final List<MessageData> sentMessages = new ArrayList<>();
    final Responder blockChainResponder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    final Responder responder =
        RespondingEthPeer.wrapResponderWithCollector(blockChainResponder, sentMessages);

    while (!result.isDone()) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
      giveOtherThreadsAGo();
    }

    // Check that known code was not requested
    final List<Bytes32> requestedHashes =
        sentMessages.stream()
            .filter(m -> m.getCode() == EthPV63.GET_NODE_DATA)
            .map(GetNodeDataMessage::readFrom)
            .flatMap(m -> StreamSupport.stream(m.hashes().spliterator(), true))
            .collect(Collectors.toList());
    assertThat(requestedHashes.size()).isGreaterThan(0);
    assertThat(Collections.disjoint(requestedHashes, knownCode.keySet())).isTrue();

    // Check that all expected account data was downloaded
    final WorldStateArchive localWorldStateArchive = new WorldStateArchive(localStorage);
    final WorldState localWorldState = localWorldStateArchive.get(stateRoot).get();
    assertThat(result).isDone();
    assertAccountsMatch(localWorldState, accounts);
  }

  @Test
  public void cancelDownloader() {
    testCancellation(false);
  }

  @Test
  public void cancelDownloaderFuture() {
    testCancellation(true);
  }

  @SuppressWarnings("unchecked")
  private void testCancellation(final boolean shouldCancelFuture) {
    final EthProtocolManager ethProtocolManager = EthProtocolManagerTestUtil.create();
    // Prevent the persistence service from running
    final MockExecutorService serviceExecutor =
        ((DeterministicEthScheduler) ethProtocolManager.ethContext().getScheduler())
            .mockServiceExecutor();
    serviceExecutor.setAutoRun(false);

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    dataGen.createRandomContractAccountsWithNonEmptyStorage(remoteWorldState, 20);
    final Hash stateRoot = remoteWorldState.rootHash();
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Create some peers
    final List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(5)
            .collect(Collectors.toList());

    final TaskQueue<NodeDataRequest> queue = spy(new InMemoryTaskQueue<>());
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());

    final WorldStateDownloader downloader =
        createDownloader(ethProtocolManager.ethContext(), localStorage, queue);

    final CompletableFuture<Void> result = downloader.run(header);

    // Send a few responses
    final Responder responder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);

    for (int i = 0; i < 3; i++) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
      giveOtherThreadsAGo();
    }
    assertThat(result.isDone()).isFalse(); // Sanity check

    // Reset queue so we can track interactions after the cancellation
    reset(queue);
    if (shouldCancelFuture) {
      result.cancel(true);
    } else {
      downloader.cancel();
      assertThat(result).isCancelled();
    }

    // Send some more responses after cancelling
    for (int i = 0; i < 3; i++) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
      giveOtherThreadsAGo();
    }

    // Now allow the persistence service to run which should exit immediately
    serviceExecutor.runPendingFutures();

    verify(queue, times(1)).clear();
    verify(queue, never()).dequeue();
    verify(queue, never()).enqueue(any());
    // Target world state should not be available
    assertThat(localStorage.isWorldStateAvailable(header.getStateRoot())).isFalse();
  }

  @Test
  public void doesNotRequestKnownAccountTrieNodesFromNetwork() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    final List<Account> accounts =
        dataGen.createRandomContractAccountsWithNonEmptyStorage(remoteWorldState, 20);
    final Hash stateRoot = remoteWorldState.rootHash();
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Create some peers
    final List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(5)
            .collect(Collectors.toList());

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());

    // Seed local storage with some trie node values
    final Map<Bytes32, BytesValue> allNodes =
        collectTrieNodesToBeRequestedAfterRoot(remoteStorage, remoteWorldState.rootHash(), 5);
    final Set<Bytes32> knownNodes = new HashSet<>();
    final Set<Bytes32> unknownNodes = new HashSet<>();
    assertThat(allNodes.size()).isGreaterThan(0); // Sanity check
    final Updater localStorageUpdater = localStorage.updater();
    final AtomicBoolean storeNode = new AtomicBoolean(true);
    allNodes.forEach(
        (nodeHash, node) -> {
          if (storeNode.get()) {
            localStorageUpdater.putAccountStateTrieNode(nodeHash, node);
            knownNodes.add(nodeHash);
          } else {
            unknownNodes.add(nodeHash);
          }
          storeNode.set(!storeNode.get());
        });
    localStorageUpdater.commit();

    final WorldStateDownloader downloader =
        createDownloader(ethProtocolManager.ethContext(), localStorage, queue);

    final CompletableFuture<Void> result = downloader.run(header);

    // Respond to node data requests
    final List<MessageData> sentMessages = new ArrayList<>();
    final Responder blockChainResponder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    final Responder responder =
        RespondingEthPeer.wrapResponderWithCollector(blockChainResponder, sentMessages);

    while (!result.isDone()) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
      giveOtherThreadsAGo();
    }

    // Check that unknown trie nodes were requested
    final List<Bytes32> requestedHashes =
        sentMessages.stream()
            .filter(m -> m.getCode() == EthPV63.GET_NODE_DATA)
            .map(GetNodeDataMessage::readFrom)
            .flatMap(m -> StreamSupport.stream(m.hashes().spliterator(), true))
            .collect(Collectors.toList());
    assertThat(requestedHashes.size()).isGreaterThan(0);
    assertThat(requestedHashes).containsAll(unknownNodes);
    assertThat(requestedHashes).doesNotContainAnyElementsOf(knownNodes);

    // Check that all expected account data was downloaded
    final WorldStateArchive localWorldStateArchive = new WorldStateArchive(localStorage);
    final WorldState localWorldState = localWorldStateArchive.get(stateRoot).get();
    assertThat(result).isDone();
    assertAccountsMatch(localWorldState, accounts);
  }

  @Test
  public void doesNotRequestKnownStorageTrieNodesFromNetwork() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    final List<Account> accounts =
        dataGen.createRandomContractAccountsWithNonEmptyStorage(remoteWorldState, 20);
    final Hash stateRoot = remoteWorldState.rootHash();
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Create some peers
    final List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(5)
            .collect(Collectors.toList());

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());

    // Seed local storage with some trie node values
    final List<Bytes32> storageRootHashes =
        new StoredMerklePatriciaTrie<>(
                remoteStorage::getNodeData,
                remoteWorldState.rootHash(),
                Function.identity(),
                Function.identity())
            .entriesFrom(Bytes32.ZERO, 5).values().stream()
                .map(RLP::input)
                .map(StateTrieAccountValue::readFrom)
                .map(StateTrieAccountValue::getStorageRoot)
                .collect(Collectors.toList());
    final Map<Bytes32, BytesValue> allTrieNodes = new HashMap<>();
    final Set<Bytes32> knownNodes = new HashSet<>();
    final Set<Bytes32> unknownNodes = new HashSet<>();
    for (final Bytes32 storageRootHash : storageRootHashes) {
      allTrieNodes.putAll(
          collectTrieNodesToBeRequestedAfterRoot(remoteStorage, storageRootHash, 5));
    }
    assertThat(allTrieNodes.size()).isGreaterThan(0); // Sanity check
    final Updater localStorageUpdater = localStorage.updater();
    boolean storeNode = true;
    for (final Entry<Bytes32, BytesValue> entry : allTrieNodes.entrySet()) {
      final Bytes32 hash = entry.getKey();
      final BytesValue data = entry.getValue();
      if (storeNode) {
        localStorageUpdater.putAccountStorageTrieNode(hash, data);
        knownNodes.add(hash);
      } else {
        unknownNodes.add(hash);
      }
      storeNode = !storeNode;
    }
    localStorageUpdater.commit();

    final WorldStateDownloader downloader =
        createDownloader(ethProtocolManager.ethContext(), localStorage, queue);

    final CompletableFuture<Void> result = downloader.run(header);

    // Respond to node data requests
    final List<MessageData> sentMessages = new ArrayList<>();
    final Responder blockChainResponder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    final Responder responder =
        RespondingEthPeer.wrapResponderWithCollector(blockChainResponder, sentMessages);

    while (!result.isDone()) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
      giveOtherThreadsAGo();
    }
    // World state should be available by the time the result is complete
    assertThat(localStorage.isWorldStateAvailable(stateRoot)).isTrue();

    // Check that unknown trie nodes were requested
    final List<Bytes32> requestedHashes =
        sentMessages.stream()
            .filter(m -> m.getCode() == EthPV63.GET_NODE_DATA)
            .map(GetNodeDataMessage::readFrom)
            .flatMap(m -> StreamSupport.stream(m.hashes().spliterator(), true))
            .collect(Collectors.toList());
    assertThat(requestedHashes.size()).isGreaterThan(0);
    assertThat(requestedHashes).containsAll(unknownNodes);
    assertThat(requestedHashes).doesNotContainAnyElementsOf(knownNodes);

    // Check that all expected account data was downloaded
    final WorldStateArchive localWorldStateArchive = new WorldStateArchive(localStorage);
    final WorldState localWorldState = localWorldStateArchive.get(stateRoot).get();
    assertThat(result).isDone();
    assertAccountsMatch(localWorldState, accounts);
  }

  @Test
  public void stalledDownloader() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    dataGen.createRandomAccounts(remoteWorldState, 10);
    final Hash stateRoot = remoteWorldState.rootHash();
    assertThat(stateRoot).isNotEqualTo(EMPTY_TRIE_ROOT); // Sanity check
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final SynchronizerConfiguration syncConfig =
        SynchronizerConfiguration.builder().worldStateRequestMaxRetries(10).build();
    final WorldStateDownloader downloader =
        createDownloader(syncConfig, ethProtocolManager.ethContext(), localStorage, queue);

    // Create a peer that can respond
    final RespondingEthPeer peer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber());

    // Start downloader (with a state root that's not available anywhere
    final CompletableFuture<?> result =
        downloader.run(
            new BlockHeaderTestFixture()
                .stateRoot(Hash.hash(BytesValue.of(1, 2, 3, 4)))
                .buildHeader());
    // A second run should return an error without impacting the first result
    final CompletableFuture<?> secondResult = downloader.run(header);
    assertThat(secondResult).isCompletedExceptionally();
    assertThat(result).isNotCompletedExceptionally();

    final Responder emptyResponder = RespondingEthPeer.emptyResponder();
    peer.respondWhileOtherThreadsWork(emptyResponder, () -> !result.isDone());

    assertThat(result).isCompletedExceptionally();
    assertThatThrownBy(result::get).hasCauseInstanceOf(StalledDownloadException.class);

    // Finally, check that when we restart the download with state that is available it works
    final CompletableFuture<Void> retryResult = downloader.run(header);
    final Responder responder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    peer.respondWhileOtherThreadsWork(responder, () -> !retryResult.isDone());
    assertThat(retryResult).isCompleted();
  }

  @Test
  public void resumesFromNonEmptyQueue() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    List<Account> accounts = dataGen.createRandomAccounts(remoteWorldState, 10);
    final Hash stateRoot = remoteWorldState.rootHash();
    assertThat(stateRoot).isNotEqualTo(EMPTY_TRIE_ROOT); // Sanity check
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Add some nodes to the queue
    final TaskQueue<NodeDataRequest> queue = spy(new InMemoryTaskQueue<>());
    List<Bytes32> queuedHashes = getFirstSetOfChildNodeRequests(remoteStorage, stateRoot);
    assertThat(queuedHashes.size()).isGreaterThan(0); // Sanity check
    for (Bytes32 bytes32 : queuedHashes) {
      queue.enqueue(new AccountTrieNodeDataRequest(Hash.wrap(bytes32)));
    }
    // Sanity check
    for (Bytes32 bytes32 : queuedHashes) {
      final Hash hash = Hash.wrap(bytes32);
      verify(queue, times(1)).enqueue(argThat((r) -> r.getHash().equals(hash)));
    }

    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final SynchronizerConfiguration syncConfig =
        SynchronizerConfiguration.builder().worldStateRequestMaxRetries(10).build();
    final WorldStateDownloader downloader =
        createDownloader(syncConfig, ethProtocolManager.ethContext(), localStorage, queue);

    // Create a peer that can respond
    final RespondingEthPeer peer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber());

    // Respond to node data requests
    final List<MessageData> sentMessages = new ArrayList<>();
    final Responder blockChainResponder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    final Responder responder =
        RespondingEthPeer.wrapResponderWithCollector(blockChainResponder, sentMessages);

    CompletableFuture<Void> result = downloader.run(header);
    while (!result.isDone()) {
      peer.respond(responder);
      giveOtherThreadsAGo();
    }
    assertThat(localStorage.isWorldStateAvailable(stateRoot)).isTrue();

    // Check that already enqueued trie nodes were requested
    final List<Bytes32> requestedHashes =
        sentMessages.stream()
            .filter(m -> m.getCode() == EthPV63.GET_NODE_DATA)
            .map(GetNodeDataMessage::readFrom)
            .flatMap(m -> StreamSupport.stream(m.hashes().spliterator(), true))
            .collect(Collectors.toList());
    assertThat(requestedHashes.size()).isGreaterThan(0);
    assertThat(requestedHashes).containsAll(queuedHashes);

    // Check that already enqueued requests were not enqueued more than once
    for (Bytes32 bytes32 : queuedHashes) {
      final Hash hash = Hash.wrap(bytes32);
      verify(queue, times(1)).enqueue(argThat((r) -> r.getHash().equals(hash)));
    }

    // Check that all expected account data was downloaded
    assertThat(result).isDone();
    final WorldStateArchive localWorldStateArchive = new WorldStateArchive(localStorage);
    final WorldState localWorldState = localWorldStateArchive.get(stateRoot).get();
    assertAccountsMatch(localWorldState, accounts);
  }

  /**
   * Walks through trie represented by the given rootHash and returns hash-node pairs that would
   * need to be requested from the network in order to reconstruct this trie, excluding the root
   * node.
   *
   * @param storage Storage holding node data required to reconstitute the trie represented by
   *     rootHash
   * @param rootHash The hash of the root node of some trie
   * @param maxNodes The maximum number of values to collect before returning
   * @return A list of hash-node pairs
   */
  private Map<Bytes32, BytesValue> collectTrieNodesToBeRequestedAfterRoot(
      final WorldStateStorage storage, final Bytes32 rootHash, final int maxNodes) {
    final Map<Bytes32, BytesValue> trieNodes = new HashMap<>();

    TrieNodeDecoder.breadthFirstDecoder(storage::getNodeData, rootHash)
        .filter(n -> !Objects.equals(n.getHash(), rootHash))
        .filter(Node::isReferencedByHash)
        .limit(maxNodes)
        .forEach((n) -> trieNodes.put(n.getHash(), n.getRlp()));

    return trieNodes;
  }

  /**
   * Returns the first set of node hashes that would need to be requested from the network after
   * retrieving the root node in order to rebuild the trie represented by the given rootHash and
   * storage.
   *
   * @param storage Storage holding node data required to reconstitute the trie represented by
   *     rootHash
   * @param rootHash The hash of the root node of some trie
   * @return A list of node hashes
   */
  private List<Bytes32> getFirstSetOfChildNodeRequests(
      final WorldStateStorage storage, final Bytes32 rootHash) {
    final List<Bytes32> hashesToRequest = new ArrayList<>();

    BytesValue rootNodeRlp = storage.getNodeData(rootHash).get();
    TrieNodeDecoder.decodeNodes(rootNodeRlp).stream()
        .filter(n -> !Objects.equals(n.getHash(), rootHash))
        .filter(Node::isReferencedByHash)
        .forEach((n) -> hashesToRequest.add(n.getHash()));

    return hashesToRequest;
  }

  private void downloadAvailableWorldStateFromPeers(
      final int peerCount,
      final int accountCount,
      final int hashesPerRequest,
      final int maxOutstandingRequests) {
    downloadAvailableWorldStateFromPeers(
        peerCount, accountCount, hashesPerRequest, maxOutstandingRequests, this::respondFully);
  }

  private void downloadAvailableWorldStateFromPeers(
      final int peerCount,
      final int accountCount,
      final int hashesPerRequest,
      final int maxOutstandingRequests,
      final NetworkResponder networkResponder) {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestUtil.create(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()));

    final int trailingPeerCount = 5;

    // Setup "remote" state
    final WorldStateStorage remoteStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive remoteWorldStateArchive = new WorldStateArchive(remoteStorage);
    final MutableWorldState remoteWorldState = remoteWorldStateArchive.getMutable();

    // Generate accounts and save corresponding state root
    final List<Account> accounts = dataGen.createRandomAccounts(remoteWorldState, accountCount);
    final Hash stateRoot = remoteWorldState.rootHash();
    assertThat(stateRoot).isNotEqualTo(EMPTY_TRIE_ROOT); // Sanity check
    final BlockHeader header =
        dataGen.block(BlockOptions.create().setStateRoot(stateRoot).setBlockNumber(10)).getHeader();

    // Generate more data that should not be downloaded
    final List<Account> otherAccounts = dataGen.createRandomAccounts(remoteWorldState, 5);
    final Hash otherStateRoot = remoteWorldState.rootHash();
    final BlockHeader otherHeader =
        dataGen
            .block(BlockOptions.create().setStateRoot(otherStateRoot).setBlockNumber(11))
            .getHeader();
    assertThat(otherStateRoot).isNotEqualTo(stateRoot); // Sanity check

    final TaskQueue<NodeDataRequest> queue = new InMemoryTaskQueue<>();
    final WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    final WorldStateArchive localWorldStateArchive = new WorldStateArchive(localStorage);
    final SynchronizerConfiguration syncConfig =
        SynchronizerConfiguration.builder()
            .worldStateHashCountPerRequest(hashesPerRequest)
            .worldStateRequestParallelism(maxOutstandingRequests)
            .build();
    final WorldStateDownloader downloader =
        createDownloader(syncConfig, ethProtocolManager.ethContext(), localStorage, queue);

    // Create some peers that can respond
    final List<RespondingEthPeer> usefulPeers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, header.getNumber()))
            .limit(peerCount)
            .collect(Collectors.toList());
    // And some irrelevant peers
    final List<RespondingEthPeer> trailingPeers =
        Stream.generate(
                () ->
                    EthProtocolManagerTestUtil.createPeer(
                        ethProtocolManager, header.getNumber() - 1L))
            .limit(trailingPeerCount)
            .collect(Collectors.toList());

    // Start downloader
    final CompletableFuture<?> result = downloader.run(header);
    // A second run should return an error without impacting the first result
    final CompletableFuture<?> secondResult = downloader.run(header);
    assertThat(secondResult).isCompletedExceptionally();
    assertThat(result).isNotCompletedExceptionally();

    // Respond to node data requests
    // Send one round of full responses, so that we can get multiple requests queued up
    final Responder fullResponder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    for (final RespondingEthPeer peer : usefulPeers) {
      peer.respond(fullResponder);
    }
    // Respond to remaining queued requests in custom way
    while (!result.isDone()) {
      networkResponder.respond(usefulPeers, remoteWorldStateArchive, result);
      giveOtherThreadsAGo();
    }

    // Check that trailing peers were not queried for data
    for (final RespondingEthPeer trailingPeer : trailingPeers) {
      assertThat(trailingPeer.hasOutstandingRequests()).isFalse();
    }

    // Check that all expected account data was downloaded
    final WorldState localWorldState = localWorldStateArchive.get(stateRoot).get();
    assertThat(result).isDone();
    assertAccountsMatch(localWorldState, accounts);

    // We shouldn't have any extra data locally
    assertThat(localStorage.contains(otherHeader.getStateRoot())).isFalse();
    for (final Account otherAccount : otherAccounts) {
      assertThat(localWorldState.get(otherAccount.getAddress())).isNull();
    }
  }

  private void respondFully(
      final List<RespondingEthPeer> peers,
      final WorldStateArchive remoteWorldStateArchive,
      final CompletableFuture<?> downloaderFuture) {
    final Responder responder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    while (!downloaderFuture.isDone()) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
      giveOtherThreadsAGo();
    }
  }

  private void respondPartially(
      final List<RespondingEthPeer> peers,
      final WorldStateArchive remoteWorldStateArchive,
      final CompletableFuture<?> downloaderFuture) {
    final Responder fullResponder =
        RespondingEthPeer.blockchainResponder(mock(Blockchain.class), remoteWorldStateArchive);
    final Responder partialResponder =
        RespondingEthPeer.partialResponder(
            mock(Blockchain.class), remoteWorldStateArchive, MainnetProtocolSchedule.create(), .5f);
    final Responder emptyResponder = RespondingEthPeer.emptyResponder();

    // Send a few partial responses
    for (int i = 0; i < 5; i++) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(partialResponder);
      }
      giveOtherThreadsAGo();
    }

    // Downloader should not complete with partial responses
    assertThat(downloaderFuture).isNotDone();

    // Send a few empty responses
    for (int i = 0; i < 3; i++) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(emptyResponder);
      }
      giveOtherThreadsAGo();
    }

    // Downloader should not complete with empty responses
    assertThat(downloaderFuture).isNotDone();

    while (!downloaderFuture.isDone()) {
      for (final RespondingEthPeer peer : peers) {
        peer.respond(fullResponder);
      }
      giveOtherThreadsAGo();
    }
  }

  private void assertAccountsMatch(
      final WorldState worldState, final List<Account> expectedAccounts) {
    for (final Account expectedAccount : expectedAccounts) {
      final Account actualAccount = worldState.get(expectedAccount.getAddress());
      assertThat(actualAccount).isNotNull();
      // Check each field
      assertThat(actualAccount.getNonce()).isEqualTo(expectedAccount.getNonce());
      assertThat(actualAccount.getCode()).isEqualTo(expectedAccount.getCode());
      assertThat(actualAccount.getBalance()).isEqualTo(expectedAccount.getBalance());

      final Map<Bytes32, UInt256> actualStorage =
          actualAccount.storageEntriesFrom(Bytes32.ZERO, 500);
      final Map<Bytes32, UInt256> expectedStorage =
          expectedAccount.storageEntriesFrom(Bytes32.ZERO, 500);
      assertThat(actualStorage).isEqualTo(expectedStorage);
    }
  }

  private WorldStateDownloader createDownloader(
      final EthContext context,
      final WorldStateStorage storage,
      final TaskQueue<NodeDataRequest> queue) {
    return createDownloader(SynchronizerConfiguration.builder().build(), context, storage, queue);
  }

  private WorldStateDownloader createDownloader(
      final SynchronizerConfiguration config,
      final EthContext context,
      final WorldStateStorage storage,
      final TaskQueue<NodeDataRequest> queue) {
    return new WorldStateDownloader(
        context,
        storage,
        queue,
        config.getWorldStateHashCountPerRequest(),
        config.getWorldStateRequestParallelism(),
        config.getWorldStateRequestMaxRetries(),
        new NoOpMetricsSystem());
  }

  private void giveOtherThreadsAGo() {
    LockSupport.parkNanos(2000);
  }

  @FunctionalInterface
  private interface NetworkResponder {
    void respond(
        final List<RespondingEthPeer> peers,
        final WorldStateArchive remoteWorldStateArchive,
        final CompletableFuture<?> downloaderFuture);
  }
}
