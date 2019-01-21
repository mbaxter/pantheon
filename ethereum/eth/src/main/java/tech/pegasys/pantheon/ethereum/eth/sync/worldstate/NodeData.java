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

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.stream.Stream;

abstract class NodeData {
  public enum Kind {
    ACCOUNT_TRIE_NODE,
    STORAGE_TRIE_NODE,
    CODE
  }

  private final Kind kind;
  private final Hash hash;
  private BytesValue data;

  protected NodeData(final Kind kind, final Hash hash) {
    this.kind = kind;
    this.hash = hash;
  }

  public static AccountTrieNodeData createAccountTrieNode(final Hash hash) {
    return new AccountTrieNodeData(hash);
  }

  public static StorageTrieNodeData createStorageTrieNode(final Hash hash) {
    return new StorageTrieNodeData(hash);
  }

  public static CodeNodeData createCodeNode(final Hash hash) {
    return new CodeNodeData(hash);
  }

  public Kind getKind() {
    return kind;
  }

  public Hash getHash() {
    return hash;
  }

  public BytesValue getData() {
    return data;
  }

  public NodeData setData(final BytesValue data) {
    this.data = data;
    return this;
  }

  abstract void persist(final WorldStateStorage.Updater updater);

  abstract Stream<NodeData> getChildNodeData();
}
