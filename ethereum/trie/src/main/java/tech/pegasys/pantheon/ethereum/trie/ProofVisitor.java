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

import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.ArrayList;
import java.util.List;

class ProofVisitor<V> extends GetVisitor<V> implements PathNodeVisitor<V> {

  private final List<Node<V>> proof = new ArrayList<>();

  @Override
  public Node<V> visit(final ExtensionNode<V> extensionNode, final BytesValue path) {
    if (extensionNode.isReferencedByHash()) {
      proof.add(extensionNode);
    }
    return super.visit(extensionNode, path);
  }

  @Override
  public Node<V> visit(final BranchNode<V> branchNode, final BytesValue path) {
    if (branchNode.isReferencedByHash()) {
      proof.add(branchNode);
    }
    return super.visit(branchNode, path);
  }

  @Override
  public Node<V> visit(final LeafNode<V> leafNode, final BytesValue path) {
    if (leafNode.isReferencedByHash()) {
      proof.add(leafNode);
    }
    return super.visit(leafNode, path);
  }

  @Override
  public Node<V> visit(final NullNode<V> nullNode, final BytesValue path) {
    return super.visit(nullNode, path);
  }

  public List<Node<V>> getProof() {
    return proof;
  }
}
