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

import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.ethereum.trie.Proof;
import tech.pegasys.pantheon.ethereum.worldstate.StateTrieAccountValue;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class WorldStateProof<K, V extends BytesValue> {

  private final StateTrieAccountValue stateTrieAccountValue;

  private final Proof<V> accountProof;

  private final Map<K, Proof<V>> storageProof;

  public WorldStateProof(
      final StateTrieAccountValue stateTrieAccountValue,
      final Proof<V> accountProof,
      final Map<K, Proof<V>> storageProof) {
    this.stateTrieAccountValue = stateTrieAccountValue;
    this.accountProof = accountProof;
    this.storageProof = storageProof;
  }

  public StateTrieAccountValue getStateTrieAccountValue() {
    return stateTrieAccountValue;
  }

  public List<BytesValue> getAccountProof() {
    return accountProof.getProofRelatedNodes();
  }

  public List<K> getStorageKeys() {
    return new ArrayList<>(storageProof.keySet());
  }

  public UInt256 getStorageValue(final K key) {
    Optional<V> value = storageProof.get(key).getValue();
    if (value.isEmpty()) {
      return UInt256.ZERO;
    } else {
      return RLP.input(value.get()).readUInt256Scalar();
    }
  }

  public List<BytesValue> getStorageProof(final K key) {
    return storageProof.get(key).getProofRelatedNodes();
  }
}
