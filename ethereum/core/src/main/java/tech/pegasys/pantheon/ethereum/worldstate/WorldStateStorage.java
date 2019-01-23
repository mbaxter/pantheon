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
package tech.pegasys.pantheon.ethereum.worldstate;

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Optional;

public interface WorldStateStorage {

  Optional<BytesValue> getCode(Hash codeHash);

  Optional<BytesValue> getAccountStateTrieNode(Bytes32 nodeHash);

  Optional<BytesValue> getAccountStorageTrieNode(Bytes32 nodeHash);

  Optional<BytesValue> getNodeData(Hash hash);

  // TODO: look into optimizing this call in implementing classes
  default boolean contains(final Hash hash) {
    return getNodeData(hash).isPresent();
  }

  Updater updater();

  interface Updater {

    void putCode(Bytes32 nodeHash, BytesValue code);

    default void putCode(final BytesValue code) {
      // Skip the hash calculation for empty code
      Hash codeHash = code.size() == 0 ? Hash.EMPTY : Hash.hash(code);
      putCode(codeHash, code);
    }

    void putAccountStateTrieNode(Bytes32 nodeHash, BytesValue node);

    void putAccountStorageTrieNode(Bytes32 nodeHash, BytesValue node);

    void commit();

    void rollback();
  }
}
