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
package tech.pegasys.pantheon.ethereum.jsonrpc.internal.methods;

import tech.pegasys.pantheon.ethereum.core.Account;
import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.MutableWorldState;
import tech.pegasys.pantheon.ethereum.jsonrpc.RpcMethod;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.JsonRpcRequest;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.BlockParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.JsonRpcParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.queries.BlockchainQueries;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcError;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcErrorResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcSuccessResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.proof.GetProofResult;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.proof.StorageEntry;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class EthGetProof implements JsonRpcMethod {

  private final BlockchainQueries blockchain;
  private final JsonRpcParameter parameters;

  public EthGetProof(final BlockchainQueries blockchain, final JsonRpcParameter parameters) {
    this.blockchain = blockchain;
    this.parameters = parameters;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_PROOF.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequest request) {
    final Address address = parameters.required(request.getParams(), 0, Address.class);
    final String[] storageKeys = parameters.required(request.getParams(), 1, String[].class);
    final long blockNumber =
        resolveBlockNumber(parameters.required(request.getParams(), 2, BlockParameter.class));

    Optional<MutableWorldState> worldState = blockchain.getWorldState(blockNumber);
    if (worldState.isPresent()) {
      Account account = worldState.get().get(address);
      if (account != null) {
        List<StorageEntry> storageEntries = new ArrayList<>();
        Arrays.stream(storageKeys)
            .forEach(
                entry ->
                    storageEntries.add(
                        new StorageEntry(
                            entry,
                            account.getStorageValue(UInt256.fromHexString(entry)),
                            account.getStorageEntry(Bytes32.fromHexString(entry)))));
        GetProofResult getProofResult =
            new GetProofResult(
                address,
                account.getBalance(),
                account.getCodeHash(),
                account.getNonce(),
                account.getStorageRoot(),
                account.getAccountProof(),
                storageEntries);
        return new JsonRpcSuccessResponse(request.getId(), getProofResult);

      } else {
        return new JsonRpcErrorResponse(request.getId(), JsonRpcError.NO_ACCOUNT_FOUND);
      }
    }

    return new JsonRpcErrorResponse(request.getId(), JsonRpcError.INTERNAL_ERROR);
  }

  private long resolveBlockNumber(final BlockParameter param) {
    if (param.getNumber().isPresent()) {
      return param.getNumber().getAsLong();
    } else if (param.isEarliest()) {
      return BlockHeader.GENESIS_BLOCK_NUMBER;
    } else if (param.isLatest() || param.isPending()) {
      return blockchain.headBlockNumber();
    } else {
      throw new IllegalStateException("Unknown block parameter type.");
    }
  }
}
