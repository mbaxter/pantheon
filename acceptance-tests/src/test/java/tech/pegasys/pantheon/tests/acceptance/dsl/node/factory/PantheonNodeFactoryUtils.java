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
package tech.pegasys.pantheon.tests.acceptance.dsl.node.factory;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static tech.pegasys.pantheon.consensus.clique.jsonrpc.CliqueRpcApis.CLIQUE;
import static tech.pegasys.pantheon.consensus.ibft.jsonrpc.IbftRpcApis.IBFT;

import tech.pegasys.pantheon.consensus.clique.CliqueExtraData;
import tech.pegasys.pantheon.consensus.ibft.IbftExtraData;
import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.jsonrpc.JsonRpcConfiguration;
import tech.pegasys.pantheon.ethereum.jsonrpc.RpcApi;
import tech.pegasys.pantheon.ethereum.jsonrpc.RpcApis;
import tech.pegasys.pantheon.ethereum.jsonrpc.websocket.WebSocketConfiguration;
import tech.pegasys.pantheon.tests.acceptance.dsl.node.GenesisConfigProvider;
import tech.pegasys.pantheon.tests.acceptance.dsl.node.RunnableNode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.io.Resources;

public class PantheonNodeFactoryUtils {

  public Optional<String> createCliqueGenesisConfig(
      final Collection<? extends RunnableNode> validators) {
    final String template = readGenesisFile("/clique/clique.json");
    return updateGenesisExtraData(
        validators, template, CliqueExtraData::createGenesisExtraDataString);
  }

  public Optional<String> createIbftGenesisConfig(
      final Collection<? extends RunnableNode> validators) {
    final String template = readGenesisFile("/ibft/ibft.json");
    return updateGenesisExtraData(
        validators, template, IbftExtraData::createGenesisExtraDataString);
  }

  public Optional<String> updateGenesisExtraData(
      final Collection<? extends RunnableNode> validators,
      final String genesisTemplate,
      final Function<List<Address>, String> extraDataCreator) {
    final List<Address> addresses =
        validators.stream().map(RunnableNode::getAddress).collect(toList());
    final String extraDataString = extraDataCreator.apply(addresses);
    final String genesis = genesisTemplate.replaceAll("%extraData%", extraDataString);
    return Optional.of(genesis);
  }

  public String readGenesisFile(final String filepath) {
    try {
      final URI uri = this.getClass().getResource(filepath).toURI();
      return Resources.toString(uri.toURL(), Charset.defaultCharset());
    } catch (final URISyntaxException | IOException e) {
      throw new IllegalStateException("Unable to get test genesis config " + filepath);
    }
  }

  public Optional<String> createGenesisConfigForValidators(
      final Collection<String> validators,
      final Collection<? extends RunnableNode> pantheonNodes,
      final GenesisConfigProvider genesisConfigProvider) {
    final List<RunnableNode> nodes =
        pantheonNodes.stream().filter(n -> validators.contains(n.getName())).collect(toList());
    return genesisConfigProvider.createGenesisConfig(nodes);
  }

  public JsonRpcConfiguration createJsonRpcConfigWithClique() {
    return createJsonRpcConfigWithRpcApiEnabled(CLIQUE);
  }

  public JsonRpcConfiguration createJsonRpcConfigWithIbft() {
    return createJsonRpcConfigWithRpcApiEnabled(IBFT);
  }

  public JsonRpcConfiguration createJsonRpcEnabledConfig() {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setEnabled(true);
    config.setPort(0);
    config.setHostsWhitelist(singletonList("*"));
    return config;
  }

  public WebSocketConfiguration createWebSocketEnabledConfig() {
    final WebSocketConfiguration config = WebSocketConfiguration.createDefault();
    config.setEnabled(true);
    config.setPort(0);
    return config;
  }

  public JsonRpcConfiguration jsonRpcConfigWithAdmin() {
    return createJsonRpcConfigWithRpcApiEnabled(RpcApis.ADMIN);
  }

  public JsonRpcConfiguration createJsonRpcConfigWithRpcApiEnabled(final RpcApi... rpcApi) {
    final JsonRpcConfiguration jsonRpcConfig = createJsonRpcEnabledConfig();
    final List<RpcApi> rpcApis = new ArrayList<>(jsonRpcConfig.getRpcApis());
    rpcApis.addAll(Arrays.asList(rpcApi));
    jsonRpcConfig.setRpcApis(rpcApis);
    return jsonRpcConfig;
  }
}
