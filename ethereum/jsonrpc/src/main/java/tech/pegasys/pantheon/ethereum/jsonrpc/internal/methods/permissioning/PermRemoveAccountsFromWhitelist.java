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
package tech.pegasys.pantheon.ethereum.jsonrpc.internal.methods.permissioning;

import tech.pegasys.pantheon.ethereum.jsonrpc.internal.JsonRpcRequest;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.methods.JsonRpcMethod;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.JsonRpcParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcError;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcErrorResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcSuccessResponse;
import tech.pegasys.pantheon.ethereum.permissioning.AccountWhitelistController;
import tech.pegasys.pantheon.ethereum.permissioning.AccountWhitelistController.RemoveResult;

import java.util.List;

public class PermRemoveAccountsFromWhitelist implements JsonRpcMethod {

  private final JsonRpcParameter parameters;
  private final AccountWhitelistController whitelistController;

  public PermRemoveAccountsFromWhitelist(
      final AccountWhitelistController whitelistController, final JsonRpcParameter parameters) {
    this.whitelistController = whitelistController;
    this.parameters = parameters;
  }

  @Override
  public String getName() {
    return "perm_removeAccountsFromWhitelist";
  }

  @Override
  @SuppressWarnings("unchecked")
  public JsonRpcResponse response(final JsonRpcRequest request) {
    final List<String> accountsList = parameters.required(request.getParams(), 0, List.class);
    final RemoveResult removeResult = whitelistController.removeAccounts(accountsList);

    switch (removeResult) {
      case ERROR_INVALID_ENTRY:
        return new JsonRpcErrorResponse(
            request.getId(), JsonRpcError.ACCOUNT_WHITELIST_INVALID_ENTRY);
      case ERROR_ABSENT_ENTRY:
        return new JsonRpcErrorResponse(
            request.getId(), JsonRpcError.ACCOUNT_WHITELIST_ABSENT_ENTRY);
      case SUCCESS:
        return new JsonRpcSuccessResponse(request.getId(), true);
      default:
        throw new IllegalStateException("Unmapped result from AccountWhitelistController");
    }
  }
}
