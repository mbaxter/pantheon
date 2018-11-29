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
package tech.pegasys.pantheon.ethereum.rlp.util;

import static java.lang.String.format;

import tech.pegasys.pantheon.ethereum.rlp.BytesValueRLPOutput;
import tech.pegasys.pantheon.ethereum.rlp.RLP;
import tech.pegasys.pantheon.ethereum.rlp.RLPException;
import tech.pegasys.pantheon.ethereum.rlp.RLPInput;
import tech.pegasys.pantheon.ethereum.rlp.RLPOutput;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;

public class RLPTestUtil {

  /**
   * Recursively decodes an RLP encoded value. Byte strings are assumed to be non-scalar (leading
   * zeros are allowed).
   *
   * @param value The RLP encoded value to decode.
   * @return The output of decoding {@code value}. It will be either directly a {@link BytesValue},
   *     or a list whose elements are either {@link BytesValue}, or similarly composed sub-lists.
   * @throws RLPException if {@code value} is not a properly formed RLP encoding.
   */
  public static Object decode(final BytesValue value) {
    return decode(RLP.input(value));
  }

  private static Object decode(final RLPInput in) {
    if (!in.nextIsList()) {
      return in.readBytesValue();
    }

    final int size = in.enterList();
    final List<Object> l = new ArrayList<>(size);
    for (int i = 0; i < size; i++) l.add(decode(in));
    in.leaveList();
    return l;
  }

  /**
   * Recursively RLP encode an object consisting of recursive lists of {@link BytesValue}.
   * BytesValues are assumed to be non-scalar (leading zeros are not trimmed).
   *
   * @param obj An object that must be either directly a {@link BytesValue}, or a list whose
   *     elements are either {@link BytesValue}, or similarly composed sub-lists.
   * @return The RLP encoding corresponding to {@code obj}.
   * @throws IllegalArgumentException if {@code obj} is not a valid input (not entirely composed
   *     from lists and {@link BytesValue}).
   */
  public static BytesValue encode(final Object obj) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    encode(obj, out);
    return out.encoded();
  }

  private static void encode(final Object obj, final RLPOutput out) {
    if (obj instanceof BytesValue) {
      out.writeBytesValue((BytesValue) obj);
    } else if (obj instanceof List) {
      final List<?> l = (List<?>) obj;
      out.startList();
      for (final Object o : l) encode(o, out);
      out.endList();
    } else {
      throw new IllegalArgumentException(
          format("Invalid input type %s for RLP encoding", obj.getClass()));
    }
  }

  /**
   * Generate a random rlp-encoded value containing a list of randomly constructed elements.
   *
   * @param randomSeed Seed to use for random generation.
   * @return a random rlp-encoded value
   */
  public static BytesValueRLPOutput randomRLPValue(int randomSeed) {
    Random random = new Random(randomSeed);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    int listDepth = 1;
    while (listDepth > 1 || (listDepth == 1 && random.nextInt(5) > 0)) {
      switch (random.nextInt(6)) {
        case 0:
          out.writeByte((byte) random.nextInt(256));
          break;
        case 1:
          out.writeShort((short) random.nextInt(0xFFFF));
          break;
        case 2:
          out.writeInt(random.nextInt());
          break;
        case 3:
          out.writeLong(random.nextLong());
          break;
        case 4:
          out.startList();
          listDepth += 1;
          break;
        case 5:
          if (listDepth > 1) {
            out.endList();
            listDepth -= 1;
          }
          break;
      }
    }
    out.endList();
    return out;
  }
}
