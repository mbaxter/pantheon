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

public interface PeerPrivileges {

  /**
   * If true, the given peer can connect or remain connected even if the max connection limit has
   * been reached or exceeded.
   *
   * @param peer The peer to be checked.
   * @return {@code true} if the peer should be allowed to connect regardless of max peer limits.
   */
  boolean canExceedMaxPeerLimits(final Peer peer);

  /**
   * If true, the given peer can initiate an incoming connection even if the remote connection limit
   * has been reached.
   *
   * @param peer The peer to be checked.
   * @return {@code true} if the peer should be allowed to connect regardless of remote connection
   *     limits.
   */
  boolean canExceedRemoteConnectionLimits(final Peer peer);
}
