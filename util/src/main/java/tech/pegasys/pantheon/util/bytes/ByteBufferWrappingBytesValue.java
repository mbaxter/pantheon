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
package tech.pegasys.pantheon.util.bytes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;

public class ByteBufferWrappingBytesValue extends AbstractBytesValue {

  protected final ByteBuffer bytes;
  protected final int offset;
  protected final int length;

  /**
   * Wraps a ByteBuffer given absolute values for offset.
   *
   * @param bytes the source byte buffer
   * @param offset the absolute offset where this value should begin
   * @param length the number of bytes to include in this value
   */
  ByteBufferWrappingBytesValue(final ByteBuffer bytes, final int offset, final int length) {
    int bytesSize = bytes.capacity();
    checkNotNull(bytes, "Invalid 'null' byte buffer provided");
    checkArgument(length >= 0, "Invalid negative length provided");
    checkElementIndex(offset, bytesSize);
    checkArgument(
        offset + length <= bytesSize,
        "Provided length %s is too big: the value has only %s bytes from offset %s",
        length,
        bytesSize - offset,
        offset);

    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int size() {
    return length;
  }

  @Override
  public byte get(final int i) {
    checkElementIndex(i, size());
    return bytes.get(i);
  }

  @Override
  public BytesValue slice(final int index, final int length) {
    if (index == 0 && length == size()) {
      return this;
    }
    if (length == 0) {
      return BytesValue.EMPTY;
    }

    checkElementIndex(index, size());
    checkArgument(
        index + length <= size(),
        "Provided length %s is too big: the value has size %s and has only %s bytes from %s",
        length,
        size(),
        size() - index,
        index);

    return new ByteBufferWrappingBytesValue(bytes, offset + index, length);
  }
}
