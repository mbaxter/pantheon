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
package tech.pegasys.pantheon.ethereum.proof;

import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.ethereum.trie.MerklePatriciaTrie;
import tech.pegasys.pantheon.ethereum.trie.Proof;
import tech.pegasys.pantheon.ethereum.trie.StoredMerklePatriciaTrie;
import tech.pegasys.pantheon.ethereum.worldstate.StateTrieAccountValue;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class WorldStateProofProvider {

  private final WorldStateStorage worldStateStorage;

  public WorldStateProofProvider(final WorldStateStorage worldStateStorage) {
    this.worldStateStorage = worldStateStorage;
  }

  public Optional<WorldStateProof<Bytes32, BytesValue>> getAccountProof(
      final Hash worldStateRoot,
      final Address accountAddress,
      final List<Bytes32> accountStorageKeys) {
    final Hash addressHash = Hash.hash(accountAddress);
    final MerklePatriciaTrie<Bytes32, BytesValue> accountStateTrie =
        newAccountStateTrie(worldStateRoot);
    return retrieveStateTrieAccountValue(addressHash, accountStateTrie)
        .map(
            stateTrieAccountValue -> {
              final MerklePatriciaTrie<Bytes32, BytesValue> storageStateTrie =
                  newAccountStorageTrie(stateTrieAccountValue.getStorageRoot());
              final Proof<BytesValue> accountProof =
                  accountStateTrie.getValueWithProof(addressHash);
              final Map<Bytes32, Proof<BytesValue>> storageProof = new HashMap<>();
              accountStorageKeys.forEach(
                  key -> storageProof.put(key, storageStateTrie.getValueWithProof(Hash.hash(key))));
              return Optional.of(
                  new WorldStateProof<>(stateTrieAccountValue, accountProof, storageProof));
            })
        .orElse(Optional.empty());
  }

  private Optional<StateTrieAccountValue> retrieveStateTrieAccountValue(
      final Hash addressHash, final MerklePatriciaTrie<Bytes32, BytesValue> accountStateTrie) {
    return accountStateTrie
        .get(addressHash)
        .map(encoded -> StateTrieAccountValue.readFrom(RLP.input(encoded)));
  }

  private MerklePatriciaTrie<Bytes32, BytesValue> newAccountStateTrie(final Bytes32 rootHash) {
    return new StoredMerklePatriciaTrie<>(
        worldStateStorage::getAccountStateTrieNode, rootHash, b -> b, b -> b);
  }

  private MerklePatriciaTrie<Bytes32, BytesValue> newAccountStorageTrie(final Bytes32 rootHash) {
    return new StoredMerklePatriciaTrie<>(
        worldStateStorage::getAccountStorageTrieNode, rootHash, b -> b, b -> b);
  }
}
