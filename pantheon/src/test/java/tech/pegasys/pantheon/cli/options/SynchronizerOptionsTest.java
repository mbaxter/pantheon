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
import tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration;

import org.junit.Test;

public class SynchronizerOptionsTest extends CommandTestAbstract {

  @Test
  public void fromConfig() {
    final SynchronizerConfiguration config = SynchronizerConfiguration.builder().build();
    SynchronizerOptions options = SynchronizerOptions.fromConfig(config);
    final SynchronizerConfiguration.Builder builderFromOptions = options.toDomainObject();
    assertThat(builderFromOptions).isEqualToComparingFieldByField(config);
  }

  @Test
  public void getCLIOptions() {
    SynchronizerOptions options = SynchronizerOptions.create();
    final String[] cliOptions = options.getCLIOptions().toArray(new String[0]);
    final SynchronizerConfiguration actualSyncConfig = options.toDomainObject().build();

    parseCommand(cliOptions);
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());
    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();
    assertThat(actualSyncConfig).isEqualToComparingFieldByField(syncConfig);

    assertThat(commandOutput.toString()).isEmpty();
    assertThat(commandErrorOutput.toString()).isEmpty();
  }

  @Test
  public void defaultValues() {
    parseCommand();

    // Check default values supplied by CLI match default SynchronizerConfiguration
    final SynchronizerConfiguration defaultConfig = SynchronizerConfiguration.builder().build();
    verify(mockControllerBuilder).synchronizerConfiguration(syncConfigurationCaptor.capture());
    final SynchronizerConfiguration syncConfig = syncConfigurationCaptor.getValue();

    // Ignore fields that are initialized in a special way
    final String[] ignoredDefaults = {"maxTrailingPeers", "computationParallelism"};

    assertThat(syncConfig).isEqualToIgnoringGivenFields(defaultConfig, ignoredDefaults);
  }
}
