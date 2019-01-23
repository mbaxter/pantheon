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
import tech.pegasys.pantheon.ethereum.trie.Node;
import tech.pegasys.pantheon.ethereum.trie.StoredNode;
import tech.pegasys.pantheon.ethereum.trie.StoredNodeFactory;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.List;
import java.util.stream.Stream;

abstract class TrieNodeData extends NodeData {

  private static final StoredNodeFactory<BytesValue> nodeFactory = StoredNodeFactory.create();

  TrieNodeData(final Kind kind, final Hash hash) {
    super(kind, hash);
  }

  @Override
  Stream<NodeData> getChildNodeData() {
    if (getData() == null) {
      // If this node hasn't been downloaded yet, we can't return any child data
      return Stream.empty();
    }

    final Node<BytesValue> node = nodeFactory.decode(getData());
    return processTrieNode(node);
  }

  private Stream<NodeData> processTrieNode(final Node<BytesValue> trieNode) {
    if (trieNode instanceof StoredNode && !((StoredNode) trieNode).isLoaded()) {
      // Stored nodes represent nodes that are referenced by hash (and therefore must be downloaded)
      NodeData req = createTrieChildNodeData(Hash.wrap(trieNode.getHash()));
      return Stream.of(req);
    }
    // Process this child's children
    final Stream<NodeData> childRequests =
        trieNode
            .getChildren()
            .map(List::stream)
            .map(s -> s.flatMap(this::processTrieNode))
            .orElse(Stream.of());

    // Process value at this node, if present
    return trieNode
        .getValue()
        .map(v -> Stream.concat(childRequests, (getNodeDataFromTrieNodeValue(v).stream())))
        .orElse(childRequests);
  }

  protected abstract NodeData createTrieChildNodeData(final Hash childHash);

  protected abstract List<NodeData> getNodeDataFromTrieNodeValue(final BytesValue value);
}
