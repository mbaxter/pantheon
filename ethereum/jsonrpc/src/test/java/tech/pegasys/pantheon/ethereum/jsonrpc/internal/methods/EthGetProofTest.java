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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.ethereum.core.Account;
import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.core.MutableWorldState;
import tech.pegasys.pantheon.ethereum.core.Wei;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.JsonRpcRequest;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.parameters.JsonRpcParameter;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.queries.BlockchainQueries;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcError;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcErrorResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.response.JsonRpcSuccessResponse;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.proof.GetProofResult;
import tech.pegasys.pantheon.ethereum.jsonrpc.internal.results.proof.StorageEntry;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EthGetProofTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Mock private BlockchainQueries blockchainQueries;
  private final JsonRpcParameter parameters = new JsonRpcParameter();

  private EthGetProof method;
  private final String JSON_RPC_VERSION = "2.0";
  private final String ETH_METHOD = "eth_getProof";

  private final Address address =
      Address.fromHexString("0x1234567890123456789012345678901234567890");
  private final String storageKey =
      "0x0000000000000000000000000000000000000000000000000000000000000001";
  private final String blockNumber = "1";

  @Before
  public void setUp() {
    method = new EthGetProof(blockchainQueries, parameters);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  public void exceptionWhenNoParamsSupplied() {
    final JsonRpcRequest request = requestWithParams();

    thrown.expect(InvalidJsonRpcParameters.class);
    thrown.expectMessage("Missing required json rpc parameter at index 0");

    method.response(request);

    verifyNoMoreInteractions(blockchainQueries);
  }

  @Test
  public void exceptionWhenNoAddressAccountSupplied() {
    final JsonRpcRequest request = requestWithParams();

    thrown.expect(InvalidJsonRpcParameters.class);
    thrown.expectMessage("Missing required json rpc parameter at index 0");

    method.response(request);
  }

  @Test
  public void exceptionWhenNoStorageKeysSupplied() {
    final JsonRpcRequest request = requestWithParams(address.toString());

    thrown.expect(InvalidJsonRpcParameters.class);
    thrown.expectMessage("Missing required json rpc parameter at index 1");

    method.response(request);
  }

  @Test
  public void exceptionWhenNoBlockNumberSupplied() {
    final JsonRpcRequest request = requestWithParams(address.toString(), new String[] {});

    thrown.expect(InvalidJsonRpcParameters.class);
    thrown.expectMessage("Missing required json rpc parameter at index 2");

    method.response(request);
  }

  @Test
  public void exceptionWhenAccountNotFound() {

    generateWorldState();

    final JsonRpcErrorResponse expectedResponse =
        new JsonRpcErrorResponse(null, JsonRpcError.NO_ACCOUNT_FOUND);
    ;

    final JsonRpcRequest request =
        requestWithParams(
            Address.fromHexString("0x0000000000000000000000000000000000000000"),
            new String[] {storageKey},
            blockNumber);

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) method.response(request);

    assertThat(response).isEqualToComparingFieldByField(expectedResponse);
  }

  @Test
  public void getProof() {

    final GetProofResult expectedResponse = generateWorldState();

    final JsonRpcRequest request =
        requestWithParams(address.toString(), new String[] {storageKey}, blockNumber);

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat(response.getResult()).isEqualToComparingFieldByFieldRecursively(expectedResponse);
  }

  private JsonRpcRequest requestWithParams(final Object... params) {
    return new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params);
  }

  private GetProofResult generateWorldState() {

    final Wei balance = Wei.of(12);
    final Hash codeHash =
        Hash.fromHexString("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    final long nonce = 1;
    final Hash storageRoot =
        Hash.fromHexString("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");
    final UInt256 storageValue = UInt256.fromHexString("0x1");
    final List<BytesValue> accountProof = new ArrayList<>();
    final List<BytesValue> storageEntries = new ArrayList<>();

    accountProof.add(BytesValue.fromHexString("0xf90211"));
    storageEntries.add(BytesValue.fromHexString("0x55"));

    final MutableWorldState mutableWorldState = mock(MutableWorldState.class);
    final Account account = mock(Account.class);

    when(account.getBalance()).thenReturn(balance);
    when(account.getCodeHash()).thenReturn(codeHash);
    when(account.getNonce()).thenReturn(nonce);
    when(account.getStorageRoot()).thenReturn(storageRoot);
    when(account.getAccountProof()).thenReturn(accountProof);
    when(account.getStorageValue(UInt256.fromHexString(storageKey))).thenReturn(storageValue);
    when(account.getStorageEntry(Bytes32.fromHexString(storageKey))).thenReturn(storageEntries);

    when(mutableWorldState.get(address)).thenReturn(account);
    when(blockchainQueries.getWorldState(Integer.parseInt(blockNumber)))
        .thenReturn(Optional.of(mutableWorldState));

    final List<StorageEntry> storageProof = new ArrayList<>();
    storageProof.add(new StorageEntry(storageKey, storageValue, storageEntries));
    return new GetProofResult(
        address, balance, codeHash, nonce, storageRoot, accountProof, storageProof);
  }
}
