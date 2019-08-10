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

import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Wei;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.Quantity;
import tech.pegasys.pantheon.ethereum.proof.WorldStateProof;
import tech.pegasys.pantheon.ethereum.worldstate.StateTrieAccountValue;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonGetter;

public class GetProofResult {

  private final List<BytesValue> accountProof;

  private final Address address;

  private final Wei balance;

  private final Bytes32 codeHash;

  private final long nonce;

  private final Bytes32 storageHash;

  private final List<StorageEntryProof> storageEntries;

  public GetProofResult(
      final Address address,
      final Wei balance,
      final Bytes32 codeHash,
      final long nonce,
      final Bytes32 storageHash,
      final List<BytesValue> accountProof,
      final List<StorageEntryProof> storageEntries) {
    this.address = address;
    this.balance = balance;
    this.codeHash = codeHash;
    this.nonce = nonce;
    this.storageHash = storageHash;
    this.accountProof = accountProof;
    this.storageEntries = storageEntries;
  }

  public static GetProofResult buildGetProofResult(
      final Address address, final WorldStateProof<Bytes32, BytesValue> worldStateProof) {

    final StateTrieAccountValue stateTrieAccountValue = worldStateProof.getStateTrieAccountValue();

    final List<StorageEntryProof> storageEntries = new ArrayList<>();
    worldStateProof
        .getStorageKeys()
        .forEach(
            key ->
                storageEntries.add(
                    new StorageEntryProof(
                        key,
                        worldStateProof.getStorageValue(key),
                        worldStateProof.getStorageProof(key))));

    return new GetProofResult(
        address,
        stateTrieAccountValue.getBalance(),
        stateTrieAccountValue.getCodeHash(),
        stateTrieAccountValue.getNonce(),
        stateTrieAccountValue.getStorageRoot(),
        worldStateProof.getAccountProof(),
        storageEntries);
  }

  @JsonGetter(value = "address")
  public String getAddress() {
    return address.toString();
  }

  @JsonGetter(value = "balance")
  public String getBalance() {
    return Quantity.create(balance);
  }

  @JsonGetter(value = "codeHash")
  public String getCodeHash() {
    return codeHash.toString();
  }

  @JsonGetter(value = "nonce")
  public String getNonce() {
    return Quantity.create(nonce);
  }

  @JsonGetter(value = "storageHash")
  public String getStorageHash() {
    return storageHash.toString();
  }

  @JsonGetter(value = "accountProof")
  public List<String> getAccountProof() {
    return accountProof.stream().map(BytesValue::toString).collect(Collectors.toList());
  }

  @JsonGetter(value = "storageProof")
  public List<StorageEntryProof> getStorageProof() {
    return storageEntries;
  }
}
