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
package tech.pegasys.pantheon.ethereum.p2p.peers;

import tech.pegasys.pantheon.util.enode.EnodeURL;

public class PeerTestHelper {

  public static Peer createPeer() {
    return DefaultPeer.fromEnodeURL(createEnode());
  }

  public static EnodeURL createEnode() {
    return EnodeURL.builder()
        .ipAddress("127.0.0.1")
        .useDefaultPorts()
        .nodeId(Peer.randomId())
        .build();
  }
}
