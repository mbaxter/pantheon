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
package tech.pegasys.pantheon.cli.options;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import tech.pegasys.pantheon.ethereum.eth.EthProtocolConfiguration;
import tech.pegasys.pantheon.util.number.PositiveNumber;

import org.junit.Test;

public class EthProtocolOptionsTest
    extends AbstractCLIOptionsTest<EthProtocolConfiguration, EthProtocolOptions> {

  @Test
  public void parsesValidEwpMaxGetHeadersOptions() {

    parseCommand("--Xewp-max-get-headers", "13");
    verify(mockControllerBuilder)
        .ethProtocolConfiguration(ethProtocolConfigurationCaptor.capture());
    final EthProtocolConfiguration config = ethProtocolConfigurationCaptor.getValue();
    assertThat(config.getMaxGetBlockHeaders()).isEqualTo(13);
    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Test
  public void parsesInvalidEwpMaxGetHeadersOptionsShouldFail() {

    parseCommand("--Xewp-max-get-headers", "-13");
    verifyZeroInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString())
        .contains(
            "Invalid value for option '--Xewp-max-get-headers': cannot convert '-13' to PositiveNumber");
  }

  @Test
  public void parsesValidEwpMaxGetBodiesOptions() {

    parseCommand("--Xewp-max-get-bodies", "14");
    verify(mockControllerBuilder)
        .ethProtocolConfiguration(ethProtocolConfigurationCaptor.capture());
    final EthProtocolConfiguration config = ethProtocolConfigurationCaptor.getValue();
    assertThat(config.getMaxGetBlockBodies()).isEqualTo(14);

    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Test
  public void parsesInvalidEwpMaxGetBodiesOptionsShouldFail() {

    parseCommand("--Xewp-max-get-bodies", "-14");
    verifyZeroInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString())
        .contains(
            "Invalid value for option '--Xewp-max-get-bodies': cannot convert '-14' to PositiveNumber");
  }

  @Test
  public void parsesValidEwpMaxGetReceiptsOptions() {

    parseCommand("--Xewp-max-get-receipts", "15");
    verify(mockControllerBuilder)
        .ethProtocolConfiguration(ethProtocolConfigurationCaptor.capture());
    final EthProtocolConfiguration config = ethProtocolConfigurationCaptor.getValue();
    assertThat(config.getMaxGetReceipts()).isEqualTo(15);

    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Test
  public void parsesInvalidEwpMaxGetReceiptsOptionsShouldFail() {

    parseCommand("--Xewp-max-get-receipts", "-15");
    verifyZeroInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString())
        .contains(
            "Invalid value for option '--Xewp-max-get-receipts': cannot convert '-15' to PositiveNumber");
  }

  @Test
  public void parsesValidEwpMaxGetNodeDataOptions() {

    parseCommand("--Xewp-max-get-node-data", "16");
    verify(mockControllerBuilder)
        .ethProtocolConfiguration(ethProtocolConfigurationCaptor.capture());
    final EthProtocolConfiguration config = ethProtocolConfigurationCaptor.getValue();
    assertThat(config.getMaxGetNodeData()).isEqualTo(16);

    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Test
  public void parsesInvalidEwpMaxGetNodeDataOptionsShouldFail() {

    parseCommand("--Xewp-max-get-node-data", "-16");
    verifyZeroInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString())
        .contains(
            "Invalid value for option '--Xewp-max-get-node-data': cannot convert '-16' to PositiveNumber");
  }

  @Override
  EthProtocolConfiguration createDefaultDomainObject() {
    return EthProtocolConfiguration.builder().build();
  }

  @Override
  EthProtocolConfiguration createCustomizedDomainObject() {
    return EthProtocolConfiguration.builder()
        .maxGetBlockHeaders(
            PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_BLOCK_HEADERS + 2))
        .maxGetBlockBodies(
            PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_BLOCK_BODIES + 2))
        .maxGetReceipts(
            PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_RECEIPTS + 2))
        .maxGetNodeData(
            PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_NODE_DATA + 2))
        .build();
  }

  @Override
  EthProtocolOptions optionsFromDomainObject(final EthProtocolConfiguration domainObject) {
    return EthProtocolOptions.fromConfig(domainObject);
  }

  @Override
  EthProtocolConfiguration optionsToDomainObject(final EthProtocolOptions options) {
    return options.toDomainObject();
  }

  @Override
  EthProtocolConfiguration getDomainObjectFromPantheonCommand() {
    verify(mockControllerBuilder)
        .ethProtocolConfiguration(ethProtocolConfigurationCaptor.capture());
    return ethProtocolConfigurationCaptor.getValue();
  }
}
