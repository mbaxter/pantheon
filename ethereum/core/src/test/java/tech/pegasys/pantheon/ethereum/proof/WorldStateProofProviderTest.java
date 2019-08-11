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
package tech.pegasys.pantheon.ethereum.proof;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.Wei;
import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.ethereum.trie.KeyValueMerkleStorage;
import tech.pegasys.pantheon.ethereum.trie.MerkleStorage;
import tech.pegasys.pantheon.ethereum.trie.StoredMerklePatriciaTrie;
import tech.pegasys.pantheon.ethereum.worldstate.StateTrieAccountValue;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.services.kvstore.InMemoryKeyValueStorage;
import tech.pegasys.pantheon.services.kvstore.KeyValueStorage;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.ArrayList;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WorldStateProofProviderTest {

  private static final Address address =
      Address.fromHexString("0x1234567890123456789012345678901234567890");

  @Mock private WorldStateStorage worldStateStorage;

  private WorldStateProofProvider worldStateProofProvider;

  @Before
  public void setup() {
    worldStateProofProvider = new WorldStateProofProvider(worldStateStorage);
  }

  @Test
  public void getProofWhenWorldStateNotAvailable() {

    when(worldStateStorage.isWorldStateAvailable(any(Bytes32.class))).thenReturn(false);

    Optional<WorldStateProof<Bytes32, BytesValue>> accountProof =
        worldStateProofProvider.getAccountProof(Hash.EMPTY, address, new ArrayList<>());

    assertThat(accountProof).isEmpty();
  }

  @Test
  public void getProofWhenWorldStateAvailable() {

    final KeyValueStorage keyValueStorage = new InMemoryKeyValueStorage();
    final MerkleStorage merkleStorage = new KeyValueMerkleStorage(keyValueStorage);

    final StateTrieAccountValue accountValue =
        new StateTrieAccountValue(1L, Wei.ZERO, Hash.EMPTY, Hash.EMPTY, 1);

    final StoredMerklePatriciaTrie<Bytes32, BytesValue> accountTrieNode =
        new StoredMerklePatriciaTrie<>(merkleStorage::get, b -> b, b -> b);
    accountTrieNode.put(Hash.hash(address), RLP.encode(accountValue::writeTo));
    accountTrieNode.commit(merkleStorage::put);
    final Bytes32 accountKeyHash = accountTrieNode.getRootHash();
    when(worldStateStorage.getAccountStateTrieNode(accountKeyHash))
        .thenReturn(merkleStorage.get(accountKeyHash));

    when(worldStateStorage.isWorldStateAvailable(any(Bytes32.class))).thenReturn(true);

    final Optional<WorldStateProof<Bytes32, BytesValue>> accountProof =
        worldStateProofProvider.getAccountProof(
            Hash.wrap(accountKeyHash), address, new ArrayList<>());

    assertThat(accountProof.get().getStateTrieAccountValue())
        .isEqualToComparingFieldByField(accountValue);
    assertThat(accountProof.get().getAccountProof()).hasSize(1);
    assertThat(accountProof.get().getAccountProof().get(0))
        .isEqualTo(merkleStorage.get(accountKeyHash).get());
  }

  @Test
  public void getProofWhenStateTrieAccountUnavailable() {

    final KeyValueStorage keyValueStorage = new InMemoryKeyValueStorage();
    final MerkleStorage merkleStorage = new KeyValueMerkleStorage(keyValueStorage);

    final StoredMerklePatriciaTrie<Bytes32, BytesValue> accountTrieNode =
        new StoredMerklePatriciaTrie<>(merkleStorage::get, b -> b, b -> b);
    accountTrieNode.commit(merkleStorage::put);
    final Bytes32 accountKeyHash = accountTrieNode.getRootHash();

    when(worldStateStorage.isWorldStateAvailable(any(Bytes32.class))).thenReturn(true);

    final Optional<WorldStateProof<Bytes32, BytesValue>> accountProof =
        worldStateProofProvider.getAccountProof(
            Hash.wrap(accountKeyHash), address, new ArrayList<>());

    assertThat(accountProof).isEmpty();
  }
}
