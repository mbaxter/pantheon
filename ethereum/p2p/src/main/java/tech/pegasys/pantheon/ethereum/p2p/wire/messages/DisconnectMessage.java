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
package tech.pegasys.pantheon.ethereum.p2p.wire.messages;

import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.wire.AbstractMessageData;
import tech.pegasys.pantheon.ethereum.rlp.BytesValueRLPOutput;
import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.ethereum.rlp.RLPInput;
import tech.pegasys.pantheon.ethereum.rlp.RLPOutput;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Optional;
import java.util.stream.Stream;

public final class DisconnectMessage extends AbstractMessageData {

  private DisconnectMessage(final BytesValue data) {
    super(data);
  }

  public static DisconnectMessage create(final DisconnectReason reason) {
    final Data data = new Data(reason);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    data.writeTo(out);

    return new DisconnectMessage(out.encoded());
  }

  public static DisconnectMessage readFrom(final MessageData message) {
    if (message instanceof DisconnectMessage) {
      return (DisconnectMessage) message;
    }
    final int code = message.getCode();
    if (code != WireMessageCodes.DISCONNECT) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a DisconnectMessage.", code));
    }
    return new DisconnectMessage(message.getData());
  }

  @Override
  public int getCode() {
    return WireMessageCodes.DISCONNECT;
  }

  public DisconnectReason getReason() {
    return Data.readFrom(RLP.input(data)).getReason();
  }

  @Override
  public String toString() {
    return "DisconnectMessage{" + "data=" + data + '}';
  }

  public static class Data {
    private final DisconnectReason reason;

    public Data(final DisconnectReason reason) {
      this.reason = reason;
    }

    public void writeTo(final RLPOutput out) {
      out.startList();
      out.writeBytesValue(reason.getValue());
      out.endList();
    }

    public static Data readFrom(final RLPInput in) {
      in.enterList();
      BytesValue reasonData = in.readBytesValue();
      in.leaveList();

      // Disconnect reason should be at most 1 byte, otherwise, just return UNKNOWN
      final DisconnectReason reason =
          reasonData.size() == 1
              ? DisconnectReason.forCode(reasonData.get(0))
              : DisconnectReason.UNKNOWN;

      return new Data(reason);
    }

    public DisconnectReason getReason() {
      return reason;
    }
  }

  /**
   * Reasons for disconnection, modelled as specified in the wire protocol DISCONNECT message.
   *
   * @see <a href="https://github.com/ethereum/wiki/wiki/%C3%90%CE%9EVp2p-Wire-Protocol">ÐΞVp2p Wire
   *     Protocol</a>
   */
  public enum DisconnectReason {
    UNKNOWN(null),
    REQUESTED((byte) 0x00),
    TCP_SUBSYSTEM_ERROR((byte) 0x01),
    BREACH_OF_PROTOCOL((byte) 0x02),
    USELESS_PEER((byte) 0x03),
    TOO_MANY_PEERS((byte) 0x04),
    ALREADY_CONNECTED((byte) 0x05),
    INCOMPATIBLE_P2P_PROTOCOL_VERSION((byte) 0x06),
    NULL_NODE_ID((byte) 0x07),
    CLIENT_QUITTING((byte) 0x08),
    UNEXPECTED_ID((byte) 0x09),
    LOCAL_IDENTITY((byte) 0x0a),
    TIMEOUT((byte) 0x0b),
    SUBPROTOCOL_TRIGGERED((byte) 0x10);

    private static final DisconnectReason[] BY_ID;
    private final Optional<Byte> code;

    static {
      final int maxValue =
          Stream.of(DisconnectReason.values())
              .filter(r -> r.code.isPresent())
              .mapToInt(r -> (int) r.code.get())
              .max()
              .getAsInt();
      BY_ID = new DisconnectReason[maxValue + 1];
      Stream.of(DisconnectReason.values())
          .filter(r -> r.code.isPresent())
          .forEach(r -> BY_ID[r.code.get()] = r);
    }

    public static DisconnectReason forCode(final Byte code) {
      if (code == null || code >= BY_ID.length || code < 0 || BY_ID[code] == null) {
        // Be permissive and just return unknown if the disconnect reason is bad
        return UNKNOWN;
      }
      return BY_ID[code];
    }

    DisconnectReason(final Byte code) {
      this.code = Optional.ofNullable(code);
    }

    public BytesValue getValue() {
      return code.map(BytesValue::of).orElse(BytesValue.EMPTY);
    }

    @Override
    public String toString() {
      return getValue().toString() + " " + name();
    }
  }
}
