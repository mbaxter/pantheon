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

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.EnodeURL;

import java.util.OptionalInt;

public class PeerTestHelper {

  public static Peer mockPeer() {
    return mockPeer(
        SECP256K1.KeyPair.generate().getPublicKey().getEncodedBytes(), "127.0.0.1", 30303);
  }

  public static Peer mockPeer(final String host, final int port) {
    final BytesValue id = SECP256K1.KeyPair.generate().getPublicKey().getEncodedBytes();
    return mockPeer(id, host, port);
  }

  public static Peer mockPeer(final BytesValue id, final String host, final int port) {
    final EnodeURL enode = new EnodeURL(id, host, port, OptionalInt.empty());
    return DefaultPeer.fromEnodeURL(enode);
  }
}
