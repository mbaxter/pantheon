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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
import tech.pegasys.pantheon.ethereum.proof.WorldStateProof;
import tech.pegasys.pantheon.ethereum.trie.Proof;
import tech.pegasys.pantheon.ethereum.worldstate.StateTrieAccountValue;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateArchive;
import tech.pegasys.pantheon.ethereum.worldstate.WorldStateStorage;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Collections;
import java.util.Map;
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
  @Mock private WorldStateStorage worldStateStorage;

  private final JsonRpcParameter parameters = new JsonRpcParameter();

  private EthGetProof method;
  private final String JSON_RPC_VERSION = "2.0";
  private final String ETH_METHOD = "eth_getProof";

  private final Address address =
      Address.fromHexString("0x1234567890123456789012345678901234567890");
  private final String storageKey =
      "0x0000000000000000000000000000000000000000000000000000000000000001";
  private final long blockNumber = 1;

  @Before
  public void setUp() {

    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
    when(worldStateArchive.getWorldStateStorage()).thenReturn(worldStateStorage);
    when(blockchainQueries.getWorldStateArchive()).thenReturn(worldStateArchive);

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

    final JsonRpcRequest request =
        requestWithParams(
            Address.fromHexString("0x0000000000000000000000000000000000000000"),
            new String[] {storageKey},
            String.valueOf(blockNumber));

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) method.response(request);

    assertThat(response).isEqualToComparingFieldByField(expectedResponse);
  }

  @Test
  public void getProof() {

    final GetProofResult expectedResponse = generateWorldState();

    final JsonRpcRequest request =
        requestWithParams(
            address.toString(), new String[] {storageKey}, String.valueOf(blockNumber));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat(response.getResult()).isEqualToComparingFieldByFieldRecursively(expectedResponse);
  }

  private JsonRpcRequest requestWithParams(final Object... params) {
    return new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params);
  }

  private GetProofResult generateWorldState() {

    final Wei balance = Wei.of(1);
    final Hash codeHash =
        Hash.fromHexString("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    final long nonce = 1;
    final Hash rootHash =
        Hash.fromHexString("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b431");
    final Hash storageRoot =
        Hash.fromHexString("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");

    final StateTrieAccountValue stateTrieAccountValue = mock(StateTrieAccountValue.class);
    when(stateTrieAccountValue.getBalance()).thenReturn(balance);
    when(stateTrieAccountValue.getCodeHash()).thenReturn(codeHash);
    when(stateTrieAccountValue.getNonce()).thenReturn(nonce);
    when(stateTrieAccountValue.getStorageRoot()).thenReturn(storageRoot);

    final Proof<BytesValue> accountProof =
        new Proof<>(
            Collections.singletonList(
                BytesValue.fromHexString(
                    "0x1111111111111111111111111111111111111111111111111111111111111111")));
    final Proof<BytesValue> storageProof =
        new Proof<>(
            Collections.singletonList(
                BytesValue.fromHexString(
                    "0x2222222222222222222222222222222222222222222222222222222222222222")));
    final WorldStateProof<Bytes32, BytesValue> worldStateProof =
        new WorldStateProof<>(
            stateTrieAccountValue,
            accountProof,
            Map.of(Bytes32.fromHexString(storageKey), storageProof));
    when(worldStateStorage.getAccountProof(eq(rootHash), eq(address), anyList()))
        .thenReturn(Optional.of(worldStateProof));

    final MutableWorldState mutableWorldState = mock(MutableWorldState.class);
    when(mutableWorldState.rootHash()).thenReturn(rootHash);
    when(blockchainQueries.getWorldState(blockNumber)).thenReturn(Optional.of(mutableWorldState));

    return GetProofResult.buildGetProofResult(address, worldStateProof);
  }
}
