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
  public Node<V> visit(final BranchNode<V> branchNode, final BytesValue path) {
    assert path.size() > 0 : "Visiting path doesn't end with a non-matching terminator";

    if (proof.isEmpty()) {
      proof.add(branchNode);
    }

    final byte childIndex = path.get(0);
    if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
      return branchNode;
    }

    Node<V> children = branchNode.child(childIndex);

    if (!(children instanceof NullNode)) {
      proof.add(children);
    }

    return children.accept(this, path.slice(1));
  }

  public List<Node<V>> getProof() {
    return proof;
  }
}
