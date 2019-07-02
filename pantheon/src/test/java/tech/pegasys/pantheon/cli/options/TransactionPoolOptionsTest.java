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

import tech.pegasys.pantheon.ethereum.eth.transactions.TransactionPoolConfiguration;

import org.junit.Test;

public class TransactionPoolOptionsTest
    extends AbstractCLIOptionsTest<TransactionPoolConfiguration, TransactionPoolOptions> {

  @Test
  public void txMessageKeepAliveSeconds() {
    final int txMessageKeepAliveSeconds = 999;
    parseCommand(
        "--Xincoming-tx-messages-keep-alive-seconds", String.valueOf(txMessageKeepAliveSeconds));

    final TransactionPoolConfiguration config = getDomainObjectFromPantheonCommand();
    assertThat(config.getTxMessageKeepAliveSeconds()).isEqualTo(txMessageKeepAliveSeconds);

    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Override
  TransactionPoolConfiguration createDefaultDomainObject() {
    return TransactionPoolConfiguration.builder().build();
  }

  @Override
  TransactionPoolConfiguration createCustomizedDomainObject() {
    return TransactionPoolConfiguration.builder()
        .txMessageKeepAliveSeconds(TransactionPoolConfiguration.DEFAULT_TX_MSG_KEEP_ALIVE + 1)
        .build();
  }

  @Override
  TransactionPoolOptions optionsFromDomainObject(final TransactionPoolConfiguration domainObject) {
    return TransactionPoolOptions.fromConfig(domainObject);
  }

  @Override
  TransactionPoolConfiguration optionsToDomainObject(final TransactionPoolOptions options) {
    return options.toDomainObject().build();
  }

  @Override
  TransactionPoolConfiguration getDomainObjectFromPantheonCommand() {
    verify(mockControllerBuilder)
        .transactionPoolConfiguration(transactionPoolConfigCaptor.capture());
    return transactionPoolConfigCaptor.getValue();
  }
}