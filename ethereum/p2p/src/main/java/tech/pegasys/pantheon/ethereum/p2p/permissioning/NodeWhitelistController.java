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
package tech.pegasys.pantheon.ethereum.p2p.permissioning;

import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.permissioning.PermissioningConfiguration;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

public class NodeWhitelistController {

  private final List<Peer> nodesWhitelist = new ArrayList<>();
  private boolean nodeWhitelistSet = false;

  public NodeWhitelistController(final PermissioningConfiguration configuration) {
    if (configuration.isNodeWhitelistSet() && configuration.getNodeWhitelist() != null) {
      for (URI uri : configuration.getNodeWhitelist()) {
        nodesWhitelist.add(DefaultPeer.fromURI(uri));
      }
      nodeWhitelistSet = true;
    }
  }

  public boolean addNode(final Peer node) {
    nodeWhitelistSet = true;
    return nodesWhitelist.add(node);
  }

  private boolean removeNode(final Peer node) {
    return nodesWhitelist.remove(node);
  }

  public NodesWhitelistResult addNodes(final List<DefaultPeer> peers) {
    for (DefaultPeer peer : peers) {
      if (nodesWhitelist.contains(peer)) {
        return new NodesWhitelistResult(
            NodesWhitelistResultType.ADD_ERROR_DUPLICATED_ENTRY,
            String.format("Specified peer: %s already exists in whitelist.", peer.getId()));
      }
    }
    peers.forEach(this::addNode);
    return new NodesWhitelistResult(NodesWhitelistResultType.SUCCESS);
  }

  public NodesWhitelistResult removeNodes(final List<DefaultPeer> peers) {
    for (DefaultPeer peer : peers) {
      if (!(nodesWhitelist.contains(peer))) {
        return new NodesWhitelistResult(
            NodesWhitelistResultType.REMOVE_ERROR_ABSENT_ENTRY,
            String.format("Specified peer: %s does not exist in whitelist.", peer.getId()));
      }
    }
    peers.forEach(this::removeNode);
    return new NodesWhitelistResult(NodesWhitelistResultType.SUCCESS);
  }

  public boolean isPermitted(final Peer node) {
    return (!nodeWhitelistSet || (nodeWhitelistSet && nodesWhitelist.contains(node)));
  }

  public List<Peer> getNodesWhitelist() {
    return nodesWhitelist;
  }

  public boolean nodeWhitelistSet() {
    return nodeWhitelistSet;
  }

  public static class NodesWhitelistResult {
    private final NodesWhitelistResultType result;
    private final Optional<String> message;

    NodesWhitelistResult(final NodesWhitelistResultType fail, final String message) {
      this.result = fail;
      this.message = Optional.of(message);
    }

    @VisibleForTesting
    public NodesWhitelistResult(final NodesWhitelistResultType success) {
      this.result = success;
      this.message = Optional.empty();
    }

    public NodesWhitelistResultType result() {
      return result;
    }

    public Optional<String> message() {
      return message;
    }
  }

  public boolean contains(final Peer node) {
    return (!nodeWhitelistSet || (nodesWhitelist.contains(node)));
  }

  public enum NodesWhitelistResultType {
    SUCCESS,
    ADD_ERROR_DUPLICATED_ENTRY,
    REMOVE_ERROR_ABSENT_ENTRY
  }
}
