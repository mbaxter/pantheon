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
package tech.pegasys.pantheon.ethereum.p2p.wire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.WireMessageCodes;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Optional;

import org.junit.Test;

public class DisconnectMessageTest {

  @Test
  public void readFromWithReason() {
    MessageData messageData =
        new RawMessage(WireMessageCodes.DISCONNECT, BytesValue.fromHexString("0xC103"));
    DisconnectMessage disconnectMessage = DisconnectMessage.readFrom(messageData);

    Optional<DisconnectReason> reason = disconnectMessage.getReason();
    assertThat(reason).contains(DisconnectReason.USELESS_PEER);
  }

  @Test
  public void readFromWithNoReason() {
    MessageData messageData =
        new RawMessage(WireMessageCodes.DISCONNECT, BytesValue.fromHexString("0xC180"));
    DisconnectMessage disconnectMessage = DisconnectMessage.readFrom(messageData);

    Optional<DisconnectReason> reason = disconnectMessage.getReason();
    assertThat(reason).isNotPresent();
  }

  @Test
  public void readFromWithWrongMessageType() {
    MessageData messageData =
        new RawMessage(WireMessageCodes.PING, BytesValue.fromHexString("0xC103"));
    assertThatThrownBy(() -> DisconnectMessage.readFrom(messageData))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Message has code 2 and thus is not a DisconnectMessage");
  }

  @Test
  public void createWithReason() {
    DisconnectMessage disconnectMessage = DisconnectMessage.create(DisconnectReason.USELESS_PEER);

    assertThat(disconnectMessage.getReason()).contains(DisconnectReason.USELESS_PEER);
    assertThat(disconnectMessage.getData().toString()).isEqualToIgnoringCase("0xC103");
  }

  @Test
  public void createWithNoReason() {
    DisconnectMessage disconnectMessage = DisconnectMessage.create();

    assertThat(disconnectMessage.getReason()).isNotPresent();
    assertThat(disconnectMessage.getData().toString()).isEqualToIgnoringCase("0xC180");
  }

  @Test
  public void createWithNullReason() {
    DisconnectMessage disconnectMessage = DisconnectMessage.create(null);

    assertThat(disconnectMessage.getReason()).isNotPresent();
    assertThat(disconnectMessage.getData().toString()).isEqualToIgnoringCase("0xC180");
  }
}
