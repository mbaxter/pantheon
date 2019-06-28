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

import tech.pegasys.pantheon.cli.CommandTestAbstract;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;

import org.junit.Test;

public class NetworkingOptionsTest extends CommandTestAbstract {

  @Test
  public void fromConfig() {
    final NetworkingConfiguration config = NetworkingConfiguration.create();
    NetworkingOptions options = NetworkingOptions.fromConfig(config);

    final NetworkingConfiguration configFromOptions = options.toDomainObject();
    assertThat(configFromOptions).isEqualToComparingFieldByField(config);
  }

  @Test
  public void getCLIOptions() {
    NetworkingOptions options = NetworkingOptions.create();
    final String[] cliOptions = options.getCLIOptions().toArray(new String[0]);
    final NetworkingConfiguration actualConfig = options.toDomainObject();

    parseCommand(cliOptions);
    verify(mockRunnerBuilder)
        .networkingConfiguration(networkingConfigurationArgumentCaptor.capture());
    final NetworkingConfiguration networkingConfiguration =
        networkingConfigurationArgumentCaptor.getValue();
    assertThat(actualConfig).isEqualToComparingFieldByField(networkingConfiguration);

    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Test
  public void defaultValues() {
    parseCommand();

    // Check default values supplied by CLI match default config
    final NetworkingConfiguration defaultConfig = NetworkingConfiguration.create();
    verify(mockRunnerBuilder)
        .networkingConfiguration(networkingConfigurationArgumentCaptor.capture());
    final NetworkingConfiguration networkingConfiguration =
        networkingConfigurationArgumentCaptor.getValue();

    assertThat(networkingConfiguration).isEqualToComparingFieldByField(defaultConfig);
  }
}
