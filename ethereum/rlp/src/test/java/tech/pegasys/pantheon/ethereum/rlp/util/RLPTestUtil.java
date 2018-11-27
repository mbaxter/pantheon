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

import java.util.ArrayList;
import java.util.List;

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
}
