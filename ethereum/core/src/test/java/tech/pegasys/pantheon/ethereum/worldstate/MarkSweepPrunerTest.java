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
package tech.pegasys.pantheon.ethereum.worldstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.chain.DefaultBlockchain;
import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.BlockDataGenerator;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.MutableWorldState;
import tech.pegasys.pantheon.ethereum.storage.keyvalue.WorldStateKeyValueStorage;
import tech.pegasys.pantheon.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.services.kvstore.InMemoryKeyValueStorage;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;
import org.mockito.InOrder;

public class MarkSweepPrunerTest {

  private final BlockDataGenerator gen = new BlockDataGenerator();
  private final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();

  @Test
  public void shouldMarkAllNodesInCurrentWorldState() {

    // Setup "remote" state
    final InMemoryKeyValueStorage markStorage = new InMemoryKeyValueStorage();
    final InMemoryKeyValueStorage stateStorage = new InMemoryKeyValueStorage();
    final WorldStateStorage worldStateStorage = new WorldStateKeyValueStorage(stateStorage);
    final WorldStateArchive worldStateArchive =
        new WorldStateArchive(
            worldStateStorage,
            new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage()));
    final MutableWorldState worldState = worldStateArchive.getMutable();
    final MutableBlockchain blockchain = mock(DefaultBlockchain.class);
    final BlockHeader header = mock(BlockHeader.class);

    // Generate accounts and save corresponding state root
    gen.createRandomContractAccountsWithNonEmptyStorage(worldState, 20);
    final Hash stateRoot = worldState.rootHash();

    when(blockchain.getBlockHeader(1)).thenReturn(Optional.of(header));
    when(header.getStateRoot()).thenReturn(stateRoot);

    final MarkSweepPruner pruner =
        new MarkSweepPruner(worldStateStorage, blockchain, markStorage, metricsSystem);
    pruner.mark(stateRoot);
    pruner.flushPendingMarks();

    final Set<BytesValue> keysToKeep = new HashSet<>(stateStorage.keySet());
    assertThat(markStorage.keySet()).containsExactlyInAnyOrderElementsOf(keysToKeep);

    // Generate some more nodes from a world state we didn't mark
    gen.createRandomContractAccountsWithNonEmptyStorage(worldStateArchive.getMutable(), 10);
    assertThat(stateStorage.keySet()).hasSizeGreaterThan(keysToKeep.size());

    // All those new nodes should be removed when we sweep
    pruner.sweepBefore(1);
    assertThat(stateStorage.keySet()).containsExactlyInAnyOrderElementsOf(keysToKeep);
    assertThat(markStorage.keySet()).isEmpty();
  }

  @Test
  public void shouldSweepStateRootFirst() {

    // Setup "remote" state
    final WorldStateStorage worldStateStorage =
        spy(new WorldStateKeyValueStorage(new InMemoryKeyValueStorage()));
    final MutableWorldState worldState =
        new WorldStateArchive(
                worldStateStorage,
                new WorldStatePreimageKeyValueStorage(new InMemoryKeyValueStorage()))
            .getMutable();
    final MutableBlockchain blockchain = mock(DefaultBlockchain.class);
    final BlockHeader header = mock(BlockHeader.class);

    // Generate accounts and save corresponding state root
    gen.createRandomContractAccountsWithNonEmptyStorage(worldState, 20);
    final Hash stateRoot = worldState.rootHash();

    when(blockchain.getBlockHeader(0)).thenReturn(Optional.of(header));
    when(header.getStateRoot()).thenReturn(stateRoot);

    final MarkSweepPruner pruner =
        new MarkSweepPruner(
            worldStateStorage, blockchain, new InMemoryKeyValueStorage(), metricsSystem);

    // Nothing is marked so we should sweep everything, but we need to make sure the state root goes
    // first
    pruner.sweepBefore(1);
    InOrder inOrder = inOrder(worldStateStorage);
    inOrder.verify(worldStateStorage).remove(stateRoot);
    inOrder.verify(worldStateStorage).removeUnless(any());
  }
}
