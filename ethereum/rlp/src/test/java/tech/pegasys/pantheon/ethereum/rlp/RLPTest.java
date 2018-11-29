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
package tech.pegasys.pantheon.ethereum.rlp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import tech.pegasys.pantheon.ethereum.rlp.util.RLPTestUtil;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.bytes.BytesValues;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.junit.Test;

public class RLPTest {

  @Test
  public void calculateSize_singleByteValue() {
    int size = RLP.calculateSize(BytesValue.fromHexString("0x01"));
    assertThat(size).isEqualTo(1);
  }

  @Test
  public void calculateSize_minSingleByteValue() {
    int size = RLP.calculateSize(BytesValue.fromHexString("0x00"));
    assertThat(size).isEqualTo(1);
  }

  @Test
  public void calculateSize_maxSingleByteValue() {
    int size = RLP.calculateSize(BytesValue.fromHexString("0x7F"));
    assertThat(size).isEqualTo(1);
  }

  @Test
  public void calculateSize_smallByteString() {
    // Prefix indicates a payload of size 5, with a 1 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0x85"));
    assertThat(size).isEqualTo(6);
  }

  @Test
  public void calculateSize_nullByteString() {
    // Prefix indicates a payload of size 0, with a 1 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0x80"));
    assertThat(size).isEqualTo(1);
  }

  @Test
  public void calculateSize_minNonNullSmallByteString() {
    // Prefix indicates a payload of size 1, with a 1 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0x81"));
    assertThat(size).isEqualTo(2);
  }

  @Test
  public void calculateSize_maxSmallByteString() {
    // Prefix indicates a payload of size 55, with a 1 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0xB7"));
    assertThat(size).isEqualTo(56);
  }

  @Test
  public void calculateSize_longByteString() {
    // Prefix indicates a payload of 56 bytes, with a 2 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0xB838"));
    assertThat(size).isEqualTo(58);
  }

  @Test
  public void calculateSize_longByteStringWithMultiByteSize() {
    // Prefix indicates a payload of 258 bytes, with a 3 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0xB90102"));
    assertThat(size).isEqualTo(261);
  }

  @Test
  public void calculateSize_shortList() {
    // Prefix indicates a payload of 5 bytes, with a 1 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0xC5"));
    assertThat(size).isEqualTo(6);
  }

  @Test
  public void calculateSize_emptyList() {
    int size = RLP.calculateSize(BytesValue.fromHexString("0xC0"));
    assertThat(size).isEqualTo(1);
  }

  @Test
  public void calculateSize_minNonEmptyList() {
    int size = RLP.calculateSize(BytesValue.fromHexString("0xC1"));
    assertThat(size).isEqualTo(2);
  }

  @Test
  public void calculateSize_maxShortList() {
    int size = RLP.calculateSize(BytesValue.fromHexString("0xF7"));
    assertThat(size).isEqualTo(56);
  }

  @Test
  public void calculateSize_longList() {
    // Prefix indicates a payload of 56 bytes, with a 2 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0xF838"));
    assertThat(size).isEqualTo(58);
  }

  @Test
  public void calculateSize_longListWithMultiByteSize() {
    // Prefix indicates a payload of 258 bytes, with a 3 byte prefix
    int size = RLP.calculateSize(BytesValue.fromHexString("0xF90102"));
    assertThat(size).isEqualTo(261);
  }

  @Test
  public void fuzz() {
    final Random random = new Random(1);
    for (int i = 0; i < 1000; ++i) {
      BytesValueRLPOutput out = RLPTestUtil.randomRLPValue(random.nextInt());
      assertThat(RLP.calculateSize(out.encoded())).isEqualTo(out.encodedSize());
    }
  }

  @Test
  public void extremelyDeepNestedList() {
    final int MAX_DEPTH = 20000;

    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    int depth = 0;

    for (int i = 0; i < MAX_DEPTH; ++i) {
      out.startList();
      depth += 1;
    }
    while (depth > 0) {
      out.endList();
      --depth;
    }
    assertThat(RLP.calculateSize(out.encoded())).isEqualTo(out.encodedSize());
  }

  @Test
  public void maxRLPStringLength() {
    // Value represents a single item with an encoded payload size of MAX_VALUE - 5 and
    // 5 bytes of metadata (payload is not actually present)
    assertThat(RLP.calculateSize(h("0xBB7FFFFFFA"))).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void overflowMaxRLPStringLength() {
    // Value represents a single item with an encoded payload size of MAX_VALUE - 4 and
    // 5 bytes of metadata (payload is not actually present)
    assertThatThrownBy(() -> RLP.calculateSize(h("0xBB7FFFFFFB")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("RLP item exceeds max supported size of 2147483647: 2147483648");
  }

  @Test
  public void rlpItemSizeHoldsMaxValue() {
    // Size value encode max positive int.  So, size is decoded, but
    // RLP is malformed because the actual payload is not present
    assertThatThrownBy(() -> RLP.input(h("0xBB7FFFFFFF")).readBytesValue())
        .isInstanceOf(CorruptedRLPInputException.class)
        .hasMessageContaining("payload should start at offset 5 but input has only 5 bytes");
  }

  @Test
  public void rlpItemSizeOverflowsSignedInt() {
    // Size value encoded in 4 bytes but exceeds max positive int value
    assertThatThrownBy(() -> RLP.input(h("0xBB80000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 4 bytes exceeds max supported size of 2147483647");
  }

  @Test
  public void rlpItemSizeOverflowsInt() {
    // Size value is encoded with 5 bytes - overflowing int
    assertThatThrownBy(() -> RLP.input(h("0xBC0100000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 5 bytes exceeds max supported size of 2147483647");
  }

  @Test
  public void rlpListSizeHoldsMaxValue() {
    // Size value encode max positive int.  So, size is decoded, but
    // RLP is malformed because the actual payload is not present
    assertThatThrownBy(() -> RLP.input(h("0xFB7FFFFFFF")).readBytesValue())
        .isInstanceOf(CorruptedRLPInputException.class)
        .hasMessageContaining(
            "Input doesn't have enough data for RLP encoding: encoding advertise a payload ending at byte 2147483652 but input has size 5");
  }

  @Test
  public void rlpListSizeOverflowsSignedInt() {
    // Size value encoded in 4 bytes but exceeds max positive int value
    assertThatThrownBy(() -> RLP.input(h("0xFB80000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 4 bytes exceeds max supported size of 2147483647");
  }

  @Test
  public void rlpListSizeOverflowsInt() {
    // Size value is encoded with 5 bytes - overflowing int
    assertThatThrownBy(() -> RLP.input(h("0xFC0100000000")))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "RLP item at offset 1 with size value consuming 5 bytes exceeds max supported size of 2147483647");
  }

  @SuppressWarnings("ReturnValueIgnored")
  @Test
  public void decodeValueWithLeadingZerosAsScalar() {
    String value = "0x8200D0";

    List<Function<RLPInput, Object>> invalidDecoders =
        Arrays.asList(
            RLPInput::readBigIntegerScalar,
            RLPInput::readIntScalar,
            RLPInput::readLongScalar,
            RLPInput::readUInt256Scalar);

    for (Function<RLPInput, Object> decoder : invalidDecoders) {
      RLPInput in = RLP.input(h(value));
      assertThatThrownBy(() -> decoder.apply(in))
          .isInstanceOf(MalformedRLPInputException.class)
          .hasMessageContaining("Invalid scalar");
    }
  }

  @Test
  public void decodeValueWithLeadingZerosAsUnsignedInt() {
    RLPInput in = RLP.input(h("0x84000000D0"));
    assertThat(in.readUnsignedInt()).isEqualTo(208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsUnsignedShort() {
    RLPInput in = RLP.input(h("0x8200D0"));
    assertThat(in.readUnsignedShort()).isEqualTo(208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsSignedInt() {
    RLPInput in = RLP.input(h("0x84000000D0"));
    assertThat(in.readInt()).isEqualTo(208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsSignedLong() {
    RLPInput in = RLP.input(h("0x8800000000000000D0"));
    assertThat(in.readLong()).isEqualTo(208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsSignedShort() {
    RLPInput in = RLP.input(h("0x8200D0"));
    assertThat(in.readShort()).isEqualTo((short) 208);
  }

  @Test
  public void decodeValueWithLeadingZerosAsBytesValue() {
    RLPInput in = RLP.input(h("0x8800000000000000D0"));
    assertThat(BytesValues.extractLong(in.readBytesValue())).isEqualTo(208);
  }

  private static BytesValue h(String hex) {
    return BytesValue.fromHexString(hex);
  }
}
