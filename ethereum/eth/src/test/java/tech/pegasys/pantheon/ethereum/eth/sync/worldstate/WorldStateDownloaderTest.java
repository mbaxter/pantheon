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

import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.AccountTuple;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManager;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManagerTestUtil;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer;
import tech.pegasys.pantheon.ethereum.eth.manager.RespondingEthPeer.Responder;
import tech.pegasys.pantheon.ethereum.eth.manager.ethtaskutils.BlockchainSetupUtil;
import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.ethereum.storage.keyvalue.KeyValueStorageWorldStateStorage;
import tech.pegasys.pantheon.ethereum.trie.MerklePatriciaTrie;
import tech.pegasys.pantheon.ethereum.trie.StoredMerklePatriciaTrie;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateArchive;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.OperationTimer;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.services.kvstore.InMemoryKeyValueStorage;
import tech.pegasys.pantheon.services.queue.InMemoryQueue;
import tech.pegasys.pantheon.services.queue.Queue;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Test;

public class WorldStateDownloaderTest {

  private static WorldStateArchive remoteWorldArchive;
  private static WorldStateStorage remoteWorldStorage;
  private static MutableBlockchain remoteBlockchain;
  private static LabelledMetric<OperationTimer> ethTasksTimer;
  private static EthProtocolManager ethProtocolManager;

  @BeforeClass
  public static void setup() {
    final BlockchainSetupUtil<Void> blockchainSetupUtil = BlockchainSetupUtil.forTesting();
    blockchainSetupUtil.importAllBlocks();
    remoteBlockchain = blockchainSetupUtil.getBlockchain();
    remoteWorldArchive = blockchainSetupUtil.getWorldArchive();
    remoteWorldStorage = remoteWorldArchive.getStorage();
    ethTasksTimer = NoOpMetricsSystem.NO_OP_LABELLED_TIMER;
    ethProtocolManager = EthProtocolManagerTestUtil.create();
  }

  @Test
  public void downloadAvailableWorldStateFromPeers() {
    // Pull chain head and a prior header with a different state root
    BlockHeader chainHead = remoteBlockchain.getChainHeadHeader();
    BlockHeader header = remoteBlockchain.getBlockHeader(chainHead.getNumber() - 1).get();
    assertThat(chainHead.getStateRoot()).isNotEqualTo(header.getStateRoot());

    Queue<NodeData> queue = new InMemoryQueue<>();
    WorldStateStorage localStorage =
        new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    WorldStateDownloader downloader =
        new WorldStateDownloader(
            ethProtocolManager.ethContext(), localStorage, header, queue, ethTasksTimer);

    // Create some peers that can respond
    int peerCount = 5;
    List<RespondingEthPeer> peers =
        Stream.generate(
                () -> EthProtocolManagerTestUtil.createPeer(ethProtocolManager, remoteBlockchain))
            .limit(peerCount)
            .collect(Collectors.toList());

    // Start downloader
    CompletableFuture<?> result = downloader.run();

    // Respond to node data requests
    Responder responder =
        RespondingEthPeer.blockchainResponder(remoteBlockchain, remoteWorldArchive);
    while (!result.isDone()) {
      for (RespondingEthPeer peer : peers) {
        peer.respond(responder);
      }
    }

    assertThat(result).isDone();
    assertStorageForWorldStateMatchesExpectation(
        header.getStateRoot(), remoteWorldStorage, localStorage);
    // We shouldn't have the chain head data, since we downloaded from a prior block
    assertThat(localStorage.contains(chainHead.getStateRoot())).isFalse();
  }

  protected void assertStorageForWorldStateMatchesExpectation(
      final Hash stateRoot,
      final WorldStateStorage expectedStorage,
      final WorldStateStorage actualStorage) {
    // Get account entries
    final MerklePatriciaTrie<BytesValue, BytesValue> expectedWorldStateTrie =
        StoredMerklePatriciaTrie.create(expectedStorage::getAccountStorageTrieNode, stateRoot);
    final Map<Bytes32, BytesValue> expectedAccountEntries =
        expectedWorldStateTrie.entriesFrom(Bytes32.ZERO, 500);
    final MerklePatriciaTrie<BytesValue, BytesValue> actualWorldStateTrie =
        StoredMerklePatriciaTrie.create(actualStorage::getAccountStorageTrieNode, stateRoot);
    final Map<Bytes32, BytesValue> actualAccountEntries =
        actualWorldStateTrie.entriesFrom(Bytes32.ZERO, 500);
    // Verify account entries
    assertThat(expectedAccountEntries).isNotEmpty(); // Sanity check
    assertThat(actualAccountEntries).isEqualTo(expectedAccountEntries);

    // Extract account tuples
    List<AccountTuple> accounts =
        expectedAccountEntries
            .entrySet()
            .stream()
            .map(e -> AccountTuple.readFrom(RLP.input(e.getValue())))
            .collect(Collectors.toList());

    // Verify each account
    for (AccountTuple account : accounts) {
      // Verify storage
      final MerklePatriciaTrie<BytesValue, BytesValue> expectedStorageTrie =
          StoredMerklePatriciaTrie.create(
              expectedStorage::getAccountStorageTrieNode, account.getStorageRoot());
      final Map<Bytes32, BytesValue> expectedStorageEntries =
          expectedStorageTrie.entriesFrom(Bytes32.ZERO, 500);
      final MerklePatriciaTrie<BytesValue, BytesValue> actualStorageTrie =
          StoredMerklePatriciaTrie.create(
              actualStorage::getAccountStorageTrieNode, account.getStorageRoot());
      final Map<Bytes32, BytesValue> actualStorageEntries =
          actualStorageTrie.entriesFrom(Bytes32.ZERO, 500);
      assertThat(actualStorageEntries).isEqualTo(expectedStorageEntries);

      // Verify code is stored
      Hash codeHash = account.getCodeHash();
      assertThat(actualStorage.getCode(codeHash)).isEqualTo(expectedStorage.getCode(codeHash));
    }
  }
}
