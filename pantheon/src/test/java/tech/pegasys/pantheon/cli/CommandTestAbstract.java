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
package tech.pegasys.pantheon.cli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import tech.pegasys.pantheon.Runner;
import tech.pegasys.pantheon.RunnerBuilder;
import tech.pegasys.pantheon.cli.PublicKeySubCommand.KeyLoader;
import tech.pegasys.pantheon.controller.PantheonController;
import tech.pegasys.pantheon.controller.PantheonControllerBuilder;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.ProtocolContext;
import tech.pegasys.pantheon.ethereum.eth.EthereumWireProtocolConfiguration;
import tech.pegasys.pantheon.ethereum.eth.manager.EthProtocolManager;
import tech.pegasys.pantheon.ethereum.eth.sync.BlockBroadcaster;
import tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration;
import tech.pegasys.pantheon.ethereum.graphql.GraphQLConfiguration;
import tech.pegasys.pantheon.ethereum.jsonrpc.JsonRpcConfiguration;
import tech.pegasys.pantheon.ethereum.jsonrpc.websocket.WebSocketConfiguration;
import tech.pegasys.pantheon.ethereum.mainnet.ProtocolSchedule;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.permissioning.PermissioningConfiguration;
import tech.pegasys.pantheon.metrics.prometheus.MetricsConfiguration;
import tech.pegasys.pantheon.services.PantheonPluginContextImpl;
import tech.pegasys.pantheon.services.kvstore.RocksDbConfiguration;
import tech.pegasys.pantheon.util.BlockImporter;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.RunLast;

@RunWith(MockitoJUnitRunner.class)
public abstract class CommandTestAbstract {

  private final Logger TEST_LOGGER = LogManager.getLogger();

  protected final ByteArrayOutputStream commandOutput = new ByteArrayOutputStream();
  private final PrintStream outPrintStream = new PrintStream(commandOutput);

  protected final ByteArrayOutputStream commandErrorOutput = new ByteArrayOutputStream();
  private final PrintStream errPrintStream = new PrintStream(commandErrorOutput);
  private final HashMap<String, String> environment = new HashMap<>();

  protected @Mock RunnerBuilder mockRunnerBuilder;
  protected @Mock Runner mockRunner;

  protected @Mock PantheonController.Builder mockControllerBuilderFactory;

  protected @Mock PantheonControllerBuilder<Void> mockControllerBuilder;
  protected @Mock EthProtocolManager mockEthProtocolManager;
  protected @Mock ProtocolSchedule<Object> mockProtocolSchedule;
  protected @Mock ProtocolContext<Object> mockProtocolContext;
  protected @Mock BlockBroadcaster mockBlockBroadcaster;
  protected @Mock EthereumWireProtocolConfiguration.Builder
      mockEthereumWireProtocolConfigurationBuilder;
  protected @Mock SynchronizerConfiguration mockSyncConf;
  protected @Mock RocksDbConfiguration.Builder mockRocksDbConfBuilder;
  protected @Mock RocksDbConfiguration mockRocksDbConf;
  protected @Mock PantheonController<Object> mockController;
  protected @Mock BlockImporter mockBlockImporter;
  protected @Mock Logger mockLogger;
  protected @Mock PantheonPluginContextImpl mockPantheonPluginContext;

  protected @Captor ArgumentCaptor<Collection<BytesValue>> bytesValueCollectionCollector;
  protected @Captor ArgumentCaptor<Collection<String>> stringListArgumentCaptor;
  protected @Captor ArgumentCaptor<Path> pathArgumentCaptor;
  protected @Captor ArgumentCaptor<File> fileArgumentCaptor;
  protected @Captor ArgumentCaptor<String> stringArgumentCaptor;
  protected @Captor ArgumentCaptor<Integer> intArgumentCaptor;
  protected @Captor ArgumentCaptor<EthNetworkConfig> ethNetworkConfigArgumentCaptor;
  protected @Captor ArgumentCaptor<NetworkingConfiguration> networkingConfigurationArgumentCaptor;
  protected @Captor ArgumentCaptor<SynchronizerConfiguration> syncConfigurationCaptor;
  protected @Captor ArgumentCaptor<JsonRpcConfiguration> jsonRpcConfigArgumentCaptor;
  protected @Captor ArgumentCaptor<GraphQLConfiguration> graphQLConfigArgumentCaptor;
  protected @Captor ArgumentCaptor<WebSocketConfiguration> wsRpcConfigArgumentCaptor;
  protected @Captor ArgumentCaptor<MetricsConfiguration> metricsConfigArgumentCaptor;
  protected @Captor ArgumentCaptor<PermissioningConfiguration>
      permissioningConfigurationArgumentCaptor;

  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  @Before
  @After
  public void resetSystemProps() {
    System.setProperty("pantheon.docker", "false");
  }

  @Before
  public void initMocks() throws Exception {

    // doReturn used because of generic PantheonController
    doReturn(mockControllerBuilder).when(mockControllerBuilderFactory).fromEthNetworkConfig(any());
    when(mockControllerBuilder.synchronizerConfiguration(any())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.ethereumWireProtocolConfiguration(any()))
        .thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.rocksDbConfiguration(any())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.dataDirectory(any())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.miningParameters(any())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.maxPendingTransactions(anyInt())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.pendingTransactionRetentionPeriod(anyInt()))
        .thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.nodePrivateKeyFile(any())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.metricsSystem(any())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.privacyParameters(any())).thenReturn(mockControllerBuilder);
    when(mockControllerBuilder.clock(any())).thenReturn(mockControllerBuilder);

    // doReturn used because of generic PantheonController
    doReturn(mockController).when(mockControllerBuilder).build();
    lenient().when(mockController.getProtocolManager()).thenReturn(mockEthProtocolManager);
    lenient().when(mockController.getProtocolSchedule()).thenReturn(mockProtocolSchedule);
    lenient().when(mockController.getProtocolContext()).thenReturn(mockProtocolContext);

    when(mockEthProtocolManager.getBlockBroadcaster()).thenReturn(mockBlockBroadcaster);
    when(mockEthereumWireProtocolConfigurationBuilder.build())
        .thenReturn(EthereumWireProtocolConfiguration.defaultConfig());

    when(mockRocksDbConfBuilder.databaseDir(any())).thenReturn(mockRocksDbConfBuilder);
    when(mockRocksDbConfBuilder.build()).thenReturn(mockRocksDbConf);

    when(mockRunnerBuilder.vertx(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.pantheonController(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.discovery(anyBoolean())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.ethNetworkConfig(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.networkingConfiguration(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.p2pAdvertisedHost(anyString())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.p2pListenPort(anyInt())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.maxPeers(anyInt())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.p2pEnabled(anyBoolean())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.jsonRpcConfiguration(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.graphQLConfiguration(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.webSocketConfiguration(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.dataDir(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.bannedNodeIds(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.metricsSystem(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.metricsConfiguration(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.staticNodes(any())).thenReturn(mockRunnerBuilder);
    when(mockRunnerBuilder.build()).thenReturn(mockRunner);
  }

  // Display outputs for debug purpose
  @After
  public void displayOutput() throws IOException {
    TEST_LOGGER.info("Standard output {}", commandOutput.toString());
    TEST_LOGGER.info("Standard error {}", commandErrorOutput.toString());

    outPrintStream.close();
    commandOutput.close();

    errPrintStream.close();
    commandErrorOutput.close();
  }

  protected void setEnvironemntVariable(final String name, final String value) {
    environment.put(name, value);
  }

  protected CommandLine.Model.CommandSpec parseCommand(final String... args) {
    return parseCommand(System.in, args);
  }

  protected CommandLine.Model.CommandSpec parseCommand(
      final KeyLoader keyLoader, final String... args) {
    return parseCommand(keyLoader, System.in, args);
  }

  protected CommandLine.Model.CommandSpec parseCommand(final InputStream in, final String... args) {
    return parseCommand(f -> KeyPair.generate(), in, args);
  }

  private CommandLine.Model.CommandSpec parseCommand(
      final KeyLoader keyLoader, final InputStream in, final String... args) {
    // turn off ansi usage globally in picocli
    System.setProperty("picocli.ansi", "false");

    final TestPantheonCommand pantheonCommand =
        new TestPantheonCommand(
            mockLogger,
            mockBlockImporter,
            mockRunnerBuilder,
            mockControllerBuilderFactory,
            mockEthereumWireProtocolConfigurationBuilder,
            mockRocksDbConfBuilder,
            keyLoader,
            mockPantheonPluginContext,
            environment);

    // parse using Ansi.OFF to be able to assert on non formatted output results
    pantheonCommand.parse(
        new RunLast().useOut(outPrintStream).useAnsi(Ansi.OFF),
        pantheonCommand.exceptionHandler().useErr(errPrintStream).useAnsi(Ansi.OFF),
        in,
        args);
    return pantheonCommand.spec;
  }

  @CommandLine.Command
  static class TestPantheonCommand extends PantheonCommand {
    @CommandLine.Spec CommandLine.Model.CommandSpec spec;
    private final KeyLoader keyLoader;

    @Override
    protected KeyLoader getKeyLoader() {
      return keyLoader;
    }

    TestPantheonCommand(
        final Logger mockLogger,
        final BlockImporter mockBlockImporter,
        final RunnerBuilder mockRunnerBuilder,
        final PantheonController.Builder controllerBuilderFactory,
        final EthereumWireProtocolConfiguration.Builder mockEthereumConfigurationMockBuilder,
        final RocksDbConfiguration.Builder mockRocksDbConfBuilder,
        final KeyLoader keyLoader,
        final PantheonPluginContextImpl pantheonPluginContext,
        final Map<String, String> environment) {
      super(
          mockLogger,
          mockBlockImporter,
          mockRunnerBuilder,
          controllerBuilderFactory,
          mockEthereumConfigurationMockBuilder,
          mockRocksDbConfBuilder,
          pantheonPluginContext,
          environment);
      this.keyLoader = keyLoader;
    }
  }
}
