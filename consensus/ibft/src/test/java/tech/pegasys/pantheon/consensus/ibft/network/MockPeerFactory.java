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
package tech.pegasys.pantheon.consensus.ibft.network;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.connections.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.wire.PeerInfo;

public class MockPeerFactory {

  public static PeerConnection create(final Address address) {
    final PeerConnection peerConnection = mock(PeerConnection.class);
    final PeerInfo peerInfo = createPeerInfo(address);
    when(peerConnection.getPeerInfo()).thenReturn(peerInfo);
    return peerConnection;
  }

  public static PeerInfo createPeerInfo(final Address address) {
    final PeerInfo peerInfo = mock(PeerInfo.class);
    when(peerInfo.getAddress()).thenReturn(address);
    return peerInfo;
  }
}
