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

import tech.pegasys.pantheon.ethereum.jsonrpc.internal.JsonRpcRequest;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.filter.LogsQuery;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.FilterParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.JsonRpcParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.queries.BlockchainQueries;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcError;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcErrorResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcSuccessResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.LogsResult;

public class EthGetLogs implements JsonRpcMethod {

  private final BlockchainQueries blockchain;
  private final JsonRpcParameter parameters;

  public EthGetLogs(final BlockchainQueries blockchain, final JsonRpcParameter parameters) {
    this.blockchain = blockchain;
    this.parameters = parameters;
  }

  @Override
  public String getName() {
    return "eth_getLogs";
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequest request) {
    final FilterParameter filter =
        parameters.required(request.getParams(), 0, FilterParameter.class);
    final LogsQuery query =
        new LogsQuery.Builder()
            .addresses(filter.getAddresses())
            .topics(filter.getTopics().getTopics())
            .build();

    if (isValid(filter)) {
      return new JsonRpcErrorResponse(request.getId(), JsonRpcError.INVALID_PARAMS);
    }
    if (filter.getBlockhash() != null) {
      return new JsonRpcSuccessResponse(
          request.getId(), new LogsResult(blockchain.matchingLogs(filter.getBlockhash(), query)));
    }

    final long fromBlockNumber = filter.getFromBlock().getNumber().orElse(0);
    final long toBlockNumber = filter.getToBlock().getNumber().orElse(blockchain.headBlockNumber());

    return new JsonRpcSuccessResponse(
        request.getId(),
        new LogsResult(blockchain.matchingLogs(fromBlockNumber, toBlockNumber, query)));
  }

  private boolean isValid(final FilterParameter filter) {
    return !filter.getFromBlock().isLatest()
        && !filter.getToBlock().isLatest()
        && filter.getBlockhash() != null;
  }
}
