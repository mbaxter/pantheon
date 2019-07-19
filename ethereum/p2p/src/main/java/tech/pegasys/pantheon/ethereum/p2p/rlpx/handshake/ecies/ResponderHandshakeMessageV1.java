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
package tech.pegasys.pantheon.ethereum.p2p.rlpx.handshake.ecies;

import static com.google.common.base.Preconditions.checkArgument;
import static tech.pegasys.pantheon.ethereum.p2p.rlpx.handshake.ecies.ECIESHandshaker.NONCE_LENGTH;
import static tech.pegasys.pantheon.ethereum.p2p.rlpx.handshake.ecies.ECIESHandshaker.PUBKEY_LENGTH;
import static tech.pegasys.pantheon.ethereum.p2p.rlpx.handshake.ecies.ECIESHandshaker.TOKEN_FLAG_LENGTH;

import com.google.common.base.MoreObjects;
import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.bytes.MutableBytesValue;

/**
 * The responder's handshake message.
 *
 * <p>This message must be sent by the party who responded to the RLPX connection, in response to
 * the initiator message.
 *
 * <h3>Message structure</h3>
 *
 * The following describes the message structure:
 *
 * <pre>
 *   authRecipient -&gt; E(remote-pubk,
 *                      remote-ephemeral-pubk
 *                      || nonce
 *                      || 0x0)
 * </pre>
 *
 * @see <a href=
 *     "https://github.com/ethereum/devp2p/blob/master/rlpx.md#encrypted-handshake">Structure of the
 *     responder response</a>
 */
public final class ResponderHandshakeMessageV1 implements ResponderHandshakeMessage {
  public static final int MESSAGE_LENGTH = PUBKEY_LENGTH + NONCE_LENGTH + TOKEN_FLAG_LENGTH;

  private final SECP256K1.PublicKey ephPublicKey; // 64 bytes - uncompressed and no type byte
  private final Bytes32 nonce; // 32 bytes
  private final boolean token; // 1 byte - 0x00 or 0x01

  private ResponderHandshakeMessageV1(
      final SECP256K1.PublicKey ephPublicKey, final Bytes32 nonce, final boolean token) {
    this.ephPublicKey = ephPublicKey;
    this.nonce = nonce;
    this.token = token;
  }

  public static ResponderHandshakeMessageV1 create(
      final SECP256K1.PublicKey ephPublicKey, final Bytes32 nonce, final boolean token) {
    return new ResponderHandshakeMessageV1(ephPublicKey, nonce, token);
  }

  public static ResponderHandshakeMessageV1 decode(final BytesValue bytes) {
    checkArgument(bytes.size() == MESSAGE_LENGTH);

    final BytesValue pubk = bytes.slice(0, PUBKEY_LENGTH);
    final SECP256K1.PublicKey ephPubKey = SECP256K1.PublicKey.create(pubk);
    final Bytes32 nonce = Bytes32.wrap(bytes.slice(PUBKEY_LENGTH, NONCE_LENGTH), 0);
    final boolean token = bytes.get(PUBKEY_LENGTH + NONCE_LENGTH) == 0x01;
    return new ResponderHandshakeMessageV1(ephPubKey, nonce, token);
  }

  @Override
  public BytesValue encode() {
    final MutableBytesValue bytes = MutableBytesValue.create(MESSAGE_LENGTH);
    ephPublicKey.getEncodedBytes().copyTo(bytes, 0);
    nonce.copyTo(bytes, PUBKEY_LENGTH);
    BytesValue.of((byte) (token ? 0x01 : 0x00)).copyTo(bytes, PUBKEY_LENGTH + NONCE_LENGTH);
    return bytes;
  }

  @Override
  public SECP256K1.PublicKey getEphPublicKey() {
    return ephPublicKey;
  }

  @Override
  public Bytes32 getNonce() {
    return nonce;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("ephPublicKey", ephPublicKey.getEncodedBytes())
      .add("nonce", nonce)
      .add("token", token)
      .toString();
  }
}
