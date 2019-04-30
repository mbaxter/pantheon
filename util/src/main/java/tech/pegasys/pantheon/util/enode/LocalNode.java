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
package tech.pegasys.pantheon.util.enode;

public interface LocalNode {

  static LocalNode create() {
    return DefaultLocalNode.create();
  }

  /**
   * While node is initializing, an empty value will be returned. Once this node is up and running,
   * a {@link EnodeURL} object corresponding to this node will be returned.
   *
   * @return The {@link EnodeURL} representation associated with this node.
   */
  EnodeURL getEnode() throws NodeNotReadyException;

  /**
   * @return True if the local node is up and running and has an available {@link EnodeURL}
   *     representation.
   */
  boolean isReady();

  /**
   * When this node is up and running with a valid {@link EnodeURL} representation, the given
   * callback will be invoked. If the callback is added after this node is ready, it is invoked
   * immediately.
   *
   * @param callback The callback to run against the {@link EnodeURL} representing the local node,
   *     when the local node is ready.
   */
  void subscribeReady(ReadyCallback callback);

  interface ReadyCallback {
    void onReady(EnodeURL localNode);
  }

  class NodeNotReadyException extends RuntimeException {
    public NodeNotReadyException(final String message) {
      super(message);
    }
  }
}
