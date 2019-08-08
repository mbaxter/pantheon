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
import tech.pegasys.pantheon.ethereum.proof.WorldStateProof;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class EthGetProof implements JsonRpcMethod {

  private final BlockchainQueries blockchain;
  private final WorldStateStorage worldStateStorage;
  private final JsonRpcParameter parameters;

  public EthGetProof(final BlockchainQueries blockchain, final JsonRpcParameter parameters) {
    this.blockchain = blockchain;
    this.worldStateStorage = blockchain.getWorldStateArchive().getWorldStateStorage();
    this.parameters = parameters;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_PROOF.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequest request) {

    final Address address = parameters.required(request.getParams(), 0, Address.class);

    final List<Bytes32> storageKeys =
        Arrays.stream(parameters.required(request.getParams(), 1, String[].class))
            .map(Bytes32::fromHexString)
            .collect(Collectors.toList());

    final long blockNumber =
        resolveBlockNumber(parameters.required(request.getParams(), 2, BlockParameter.class));

    final Optional<MutableWorldState> worldState = blockchain.getWorldState(blockNumber);

    if (worldState.isPresent()) {
      Optional<WorldStateProof<Bytes32, BytesValue>> proofOptional =
          worldStateStorage.getAccountProof(worldState.get().rootHash(), address, storageKeys);
      return proofOptional
          .map(
              proof ->
                  (JsonRpcResponse)
                      new JsonRpcSuccessResponse(
                          request.getId(), GetProofResult.buildGetProofResult(address, proof)))
          .orElse(new JsonRpcErrorResponse(request.getId(), JsonRpcError.NO_ACCOUNT_FOUND));
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
