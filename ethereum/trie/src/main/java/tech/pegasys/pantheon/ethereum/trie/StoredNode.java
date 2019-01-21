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
package tech.pegasys.pantheon.ethereum.trie;

import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.List;
import java.util.Optional;

public class StoredNode<V> implements Node<V> {
  private final StoredNodeFactory<V> nodeFactory;
  private final Bytes32 hash;
  private Node<V> loaded;

  StoredNode(final StoredNodeFactory<V> nodeFactory, final Bytes32 hash) {
    this.nodeFactory = nodeFactory;
    this.hash = hash;
  }

  public boolean isLoaded() {
    return loaded != null;
  }

  /** @return True if the node needs to be persisted. */
  @Override
  public boolean isDirty() {
    return false;
  }

  /** Marks the node as being modified (needs to be persisted); */
  @Override
  public void markDirty() {
    throw new IllegalStateException(
        "A stored node cannot ever be dirty since it's loaded from storage");
  }

  @Override
  public Node<V> accept(final PathNodeVisitor<V> visitor, final BytesValue path) {
    final Node<V> node = load();
    return node.accept(visitor, path);
  }

  @Override
  public void accept(final NodeVisitor<V> visitor) {
    final Node<V> node = load();
    node.accept(visitor);
  }

  @Override
  public BytesValue getPath() {
    return load().getPath();
  }

  @Override
  public Optional<V> getValue() {
    return load().getValue();
  }

  @Override
  public Optional<List<Node<V>>> getChildren() {
    return load().getChildren();
  }

  @Override
  public BytesValue getRlp() {
    // Getting the rlp representation is only needed when persisting a concrete node
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesValue getRlpRef() {
    // If this node was stored, then it must have a rlp larger than a hash
    return RLP.encodeOne(hash);
  }

  @Override
  public Bytes32 getHash() {
    return hash;
  }

  @Override
  public Node<V> replacePath(final BytesValue path) {
    return load().replacePath(path);
  }

  private Node<V> load() {
    if (loaded == null) {
      loaded =
          nodeFactory
              .retrieve(hash)
              .orElseThrow(() -> new MerkleStorageException("Missing value for hash " + hash));
    }

    return loaded;
  }

  @Override
  public String print() {
    final String value = load().print();
    return value;
  }
}
