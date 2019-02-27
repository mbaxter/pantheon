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
package tech.pegasys.pantheon.ethereum.trie;

import static com.google.common.base.Preconditions.checkArgument;

import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class TrieNodeDecoder {

  private final StoredNodeFactory<BytesValue> nodeFactory;

  private TrieNodeDecoder(final NodeLoader nodeLoader) {
    nodeFactory = new StoredNodeFactory<>(nodeLoader, Function.identity(), Function.identity());
  }

  public static TrieNodeDecoder create(final NodeLoader nodeLoader) {
    return new TrieNodeDecoder(nodeLoader);
  }

  public static TrieNodeDecoder create() {
    return new TrieNodeDecoder((h) -> Optional.empty());
  }

  public Node<BytesValue> decode(final BytesValue rlp) {
    return nodeFactory.decode(rlp);
  }

  /**
   * Flattens this node and all of its inlined nodes and node references into a list.
   *
   * @param nodeRlp The bytes of the trie node to be decoded.
   * @return A list of nodes and node references embedded in the given rlp.
   */
  public List<Node<BytesValue>> decodeNodes(final BytesValue nodeRlp) {
    Node<BytesValue> node = decode(nodeRlp);
    List<Node<BytesValue>> nodes = new ArrayList<>();
    nodes.add(node);

    final List<Node<BytesValue>> toProcess = new ArrayList<>();
    node.getChildren().ifPresent(toProcess::addAll);
    while (!toProcess.isEmpty()) {
      final Node<BytesValue> currentNode = toProcess.remove(0);
      if (Objects.equals(NullNode.instance(), currentNode)) {
        // Skip null nodes
        continue;
      }
      nodes.add(currentNode);

      if (!currentNode.isReferencedByHash()) {
        // If current node is inlined, that means we can process its children
        currentNode.getChildren().ifPresent(toProcess::addAll);
      }
    }

    return nodes;
  }

  /**
   * Walks the trie in a bread-first manner, returning the list of nodes encountered in order. If
   * any nodes are missing from the nodeLoader, those nodes are just skipped.
   *
   * @param rootHash The hash of the root node
   * @param maxDepth The maximum depth to traverse to. A value of zero will traverse the root node
   *     only.
   * @param maxNodes The maximum number of nodes to return
   * @return A list of non-null nodes in the order they were encountered.
   */
  public List<Node<BytesValue>> breadthFirstDecode(
      final Bytes32 rootHash, final int maxDepth, final int maxNodes) {
    checkArgument(maxDepth >= 0);
    checkArgument(maxNodes >= 0);
    final List<Node<BytesValue>> trieNodes = new ArrayList<>();
    if (rootHash == MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH || maxNodes == 0) {
      return trieNodes;
    }

    final Optional<Node<BytesValue>> maybeRootNode = nodeFactory.retrieve(rootHash);
    if (!maybeRootNode.isPresent()) {
      return trieNodes;
    }
    final Node<BytesValue> rootNode = maybeRootNode.get();

    // Walk through nodes in breadth-first traversal
    List<Node<BytesValue>> currentNodes = new ArrayList<>();
    currentNodes.add(rootNode);
    List<Node<BytesValue>> nextNodes = new ArrayList<>();

    int currentDepth = 0;
    while (!currentNodes.isEmpty() && currentDepth <= maxDepth) {
      final Node<BytesValue> currentNode = currentNodes.remove(0);
      trieNodes.add(currentNode);
      if (trieNodes.size() >= maxNodes) {
        break;
      }

      final List<Node<BytesValue>> children = new ArrayList<>();
      currentNode.getChildren().ifPresent(children::addAll);
      while (!children.isEmpty()) {
        Node<BytesValue> child = children.remove(0);
        if (Objects.equals(child, NullNode.instance())) {
          // Ignore null nodes
          continue;
        }
        if (child.isReferencedByHash()) {
          // Retrieve hash-referenced child
          final Optional<Node<BytesValue>> maybeChildNode = nodeFactory.retrieve(child.getHash());
          if (!maybeChildNode.isPresent()) {
            continue;
          }
          child = maybeChildNode.get();
        }
        nextNodes.add(child);
      }

      // Set up next level
      if (currentNodes.isEmpty()) {
        currentDepth += 1;
        currentNodes = nextNodes;
        nextNodes = new ArrayList<>();
      }
    }

    return trieNodes;
  }
}
