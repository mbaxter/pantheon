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
package tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.proof;

import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.Quantity;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonGetter;

public class StorageEntry {

  private final String key;

  private final UInt256 value;

  private final List<BytesValue> storageProof;

  public StorageEntry(final String key, final UInt256 value, final List<BytesValue> storageProof) {
    this.key = key;
    this.value = value;
    this.storageProof = storageProof;
  }

  @JsonGetter(value = "key")
  public String getKey() {
    return key;
  }

  @JsonGetter(value = "value")
  public String getValue() {
    return Quantity.create(value);
  }

  @JsonGetter(value = "proof")
  public List<String> getStorageProof() {
    return storageProof.stream().map(BytesValue::toString).collect(Collectors.toList());
  }
}
