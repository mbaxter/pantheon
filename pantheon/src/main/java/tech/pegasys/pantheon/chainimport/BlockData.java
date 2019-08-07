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
package tech.pegasys.pantheon.chainimport;

import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.Transaction;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BlockData {

  private final Optional<Long> number;
  private final Optional<Hash> parentHash;
  private final List<TransactionData> transactionData;
  private final Address coinbase;
  private final BytesValue extraData;

  @JsonCreator
  public BlockData(
      @JsonProperty("number") final Optional<String> number,
      @JsonProperty("parentHash") final Optional<String> parentHash,
      @JsonProperty("coinbase") final Optional<String> coinbase,
      @JsonProperty("extraData") final Optional<String> extraData,
      @JsonProperty("transactions") final List<TransactionData> transactions) {
    this.number = number.map(UInt256::fromHexString).map(UInt256::toLong);
    this.parentHash = parentHash.map(Bytes32::fromHexString).map(Hash::wrap);
    this.coinbase = coinbase.map(Address::fromHexString).orElse(Address.ZERO);
    this.extraData = extraData.map(BytesValue::fromHexStringLenient).orElse(BytesValue.EMPTY);
    this.transactionData = transactions;
  }

  public Optional<Long> getNumber() {
    return number;
  }

  public Optional<Hash> getParentHash() {
    return parentHash;
  }

  public Address getCoinbase() {
    return coinbase;
  }

  public BytesValue getExtraData() {
    return extraData;
  }

  public Stream<Transaction> streamTransactions() {
    return transactionData.stream().map(TransactionData::getSignedTransaction);
  }
}
