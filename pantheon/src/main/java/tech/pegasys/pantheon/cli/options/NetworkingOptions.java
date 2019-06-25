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

import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;

import java.util.Arrays;
import java.util.List;

import picocli.CommandLine;

public class NetworkingOptions implements CLIOptions<NetworkingConfiguration> {
  private final String INITIATE_CONNECTIONS_FREQUENCY_FLAG =
      "--Xp2p-initiate-connections-frequency";
  private final String CHECK_MAINTAINED_CONNECTIONS_FREQUENCY_FLAG =
      "--Xp2p-check-maintained-connections-frequency";

  @CommandLine.Option(
      names = INITIATE_CONNECTIONS_FREQUENCY_FLAG,
      hidden = true,
      defaultValue = "30",
      paramLabel = "<INTEGER>",
      description =
          "The frequency (in seconds) at which to initiate new outgoing connections (default: ${DEFAULT-VALUE})")
  private int initiateConnectionsFrequencySec = 30;

  @CommandLine.Option(
      names = CHECK_MAINTAINED_CONNECTIONS_FREQUENCY_FLAG,
      hidden = true,
      defaultValue = "60",
      paramLabel = "<INTEGER>",
      description =
          "The frequency (in seconds) at which to check maintained connections (default: ${DEFAULT-VALUE})")
  private int checkMaintainedConnectionsFrequencySec = 60;

  private NetworkingOptions() {}

  public static NetworkingOptions create() {
    return new NetworkingOptions();
  }

  public static CLIOptions<NetworkingConfiguration> fromConfig(
      final NetworkingConfiguration networkingConfig) {
    final NetworkingOptions cliOptions = new NetworkingOptions();
    cliOptions.checkMaintainedConnectionsFrequencySec =
        networkingConfig.getCheckMaintainedConnectionsFrequencySec();
    cliOptions.initiateConnectionsFrequencySec =
        networkingConfig.getInitiateConnectionsFrequencySec();
    return cliOptions;
  }

  @Override
  public NetworkingConfiguration toDomainObject() {
    NetworkingConfiguration config = NetworkingConfiguration.create();
    config.setCheckMaintainedConnectionsFrequency(checkMaintainedConnectionsFrequencySec);
    config.setInitiateConnectionsFrequency(initiateConnectionsFrequencySec);
    return config;
  }

  @Override
  public List<String> getCLIOptions() {
    return Arrays.asList(
        CHECK_MAINTAINED_CONNECTIONS_FREQUENCY_FLAG,
        Integer.toString(checkMaintainedConnectionsFrequencySec, 10),
        INITIATE_CONNECTIONS_FREQUENCY_FLAG,
        Integer.toString(initiateConnectionsFrequencySec, 10));
  }
}
