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
package tech.pegasys.pantheon.cli.chainimport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import tech.pegasys.pantheon.chainimport.ChainImporter;
import tech.pegasys.pantheon.config.GenesisConfigFile;
import tech.pegasys.pantheon.config.JsonUtil;
import tech.pegasys.pantheon.controller.PantheonController;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.chain.Blockchain;
import tech.pegasys.pantheon.ethereum.core.Address;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.BlockBody;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.InMemoryStorageProvider;
import tech.pegasys.pantheon.ethereum.core.MiningParametersTestBuilder;
import tech.pegasys.pantheon.ethereum.core.PrivacyParameters;
import tech.pegasys.pantheon.ethereum.core.Transaction;
import tech.pegasys.pantheon.ethereum.core.Wei;
import tech.pegasys.pantheon.ethereum.eth.EthProtocolConfiguration;
import tech.pegasys.pantheon.ethereum.eth.sync.SynchronizerConfiguration;
import tech.pegasys.pantheon.ethereum.eth.transactions.TransactionPoolConfiguration;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.testutil.TestClock;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ChainImporterTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void importChain_validJson_withBlockNumbers() throws IOException {
    final PantheonController<?> controller = createController(getDevGenesisConfig());
    final ChainImporter<?> importer = new ChainImporter<>(controller);

    final URL jsonDataURL = ChainImporter.class.getResource("blocks-import-valid.json");
    final String jsonData = Resources.toString(jsonDataURL, UTF_8);

    importer.importChain(jsonData);

    final Blockchain blockchain = controller.getProtocolContext().getBlockchain();

    // Check blocks were imported
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(4);
    // Get imported blocks
    List<Block> blocks = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      blocks.add(getBlockAt(blockchain, i + 1));
    }

    // Check block 1
    Block block = blocks.get(0);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.EMPTY);
    assertThat(block.getHeader().getCoinbase()).isEqualTo(Address.ZERO);
    assertThat(block.getBody().getTransactions().size()).isEqualTo(2);
    // Check first tx
    Transaction tx = block.getBody().getTransactions().get(0);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("fe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    assertThat(tx.getTo())
        .hasValue(Address.fromHexString("627306090abaB3A6e1400e9345bC60c78a8BEf57"));
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFF1L);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xFF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(1L));
    assertThat(tx.getNonce()).isEqualTo(0L);
    // Check second tx
    tx = block.getBody().getTransactions().get(1);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("fe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    assertThat(tx.getTo())
        .hasValue(Address.fromHexString("f17f52151EbEF6C7334FAD080c5704D77216b732"));
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFF2L);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xEF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(0L));
    assertThat(tx.getNonce()).isEqualTo(1L);

    // Check block 2
    block = blocks.get(1);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.fromHexString("0x1234"));
    assertThat(block.getHeader().getCoinbase()).isEqualTo(Address.fromHexString("0x02"));
    assertThat(block.getBody().getTransactions().size()).isEqualTo(1);
    // Check first tx
    tx = block.getBody().getTransactions().get(0);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("627306090abaB3A6e1400e9345bC60c78a8BEf57"));
    assertThat(tx.getTo())
        .hasValue(Address.fromHexString("f17f52151EbEF6C7334FAD080c5704D77216b732"));
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFFFL);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xFF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(0L));
    assertThat(tx.getNonce()).isEqualTo(0L);

    // Check block 3
    block = blocks.get(2);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.fromHexString("0x3456"));
    assertThat(block.getHeader().getCoinbase())
        .isEqualTo(Address.fromHexString("f17f52151EbEF6C7334FAD080c5704D77216b732"));
    assertThat(block.getBody().getTransactions().size()).isEqualTo(0);

    // Check block 4
    block = blocks.get(3);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.EMPTY);
    assertThat(block.getHeader().getCoinbase()).isEqualTo(Address.ZERO);
    assertThat(block.getBody().getTransactions().size()).isEqualTo(1);
    // Check first tx
    tx = block.getBody().getTransactions().get(0);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("627306090abaB3A6e1400e9345bC60c78a8BEf57"));
    assertThat(tx.getTo()).isEmpty();
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFFFL);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xFF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(0L));
    assertThat(tx.getNonce()).isEqualTo(1L);
  }

  @Test
  public void importChain_validJson_NoBlockIdentifiers() throws IOException {
    final PantheonController<?> controller = createController(getDevGenesisConfig());
    final ChainImporter<?> importer = new ChainImporter<>(controller);

    final URL jsonDataURL =
        ChainImporter.class.getResource("blocks-import-valid-no-block-identifiers.json");
    final String jsonData = Resources.toString(jsonDataURL, UTF_8);

    importer.importChain(jsonData);

    final Blockchain blockchain = controller.getProtocolContext().getBlockchain();

    // Check blocks were imported
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(4);
    // Get imported blocks
    List<Block> blocks = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      blocks.add(getBlockAt(blockchain, i + 1));
    }

    // Check block 1
    Block block = blocks.get(0);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.EMPTY);
    assertThat(block.getHeader().getCoinbase()).isEqualTo(Address.ZERO);
    assertThat(block.getBody().getTransactions().size()).isEqualTo(2);
    // Check first tx
    Transaction tx = block.getBody().getTransactions().get(0);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("fe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    assertThat(tx.getTo())
        .hasValue(Address.fromHexString("627306090abaB3A6e1400e9345bC60c78a8BEf57"));
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFF1L);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xFF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(1L));
    assertThat(tx.getNonce()).isEqualTo(0L);
    // Check second tx
    tx = block.getBody().getTransactions().get(1);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("fe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    assertThat(tx.getTo())
        .hasValue(Address.fromHexString("f17f52151EbEF6C7334FAD080c5704D77216b732"));
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFF2L);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xEF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(0L));
    assertThat(tx.getNonce()).isEqualTo(1L);

    // Check block 2
    block = blocks.get(1);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.fromHexString("0x1234"));
    assertThat(block.getHeader().getCoinbase()).isEqualTo(Address.fromHexString("0x02"));
    assertThat(block.getBody().getTransactions().size()).isEqualTo(1);
    // Check first tx
    tx = block.getBody().getTransactions().get(0);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("627306090abaB3A6e1400e9345bC60c78a8BEf57"));
    assertThat(tx.getTo())
        .hasValue(Address.fromHexString("f17f52151EbEF6C7334FAD080c5704D77216b732"));
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFFFL);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xFF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(0L));
    assertThat(tx.getNonce()).isEqualTo(0L);

    // Check block 3
    block = blocks.get(2);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.fromHexString("0x3456"));
    assertThat(block.getHeader().getCoinbase())
        .isEqualTo(Address.fromHexString("f17f52151EbEF6C7334FAD080c5704D77216b732"));
    assertThat(block.getBody().getTransactions().size()).isEqualTo(0);

    // Check block 4
    block = blocks.get(3);
    assertThat(block.getHeader().getExtraData()).isEqualTo(BytesValue.EMPTY);
    assertThat(block.getHeader().getCoinbase()).isEqualTo(Address.ZERO);
    assertThat(block.getBody().getTransactions().size()).isEqualTo(1);
    // Check first tx
    tx = block.getBody().getTransactions().get(0);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("627306090abaB3A6e1400e9345bC60c78a8BEf57"));
    assertThat(tx.getTo()).isEmpty();
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFFFL);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xFF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(0L));
    assertThat(tx.getNonce()).isEqualTo(1L);
  }

  @Test
  public void importChain_validJson_withParentHashes() throws IOException {
    final PantheonController<?> controller = createController(getDevGenesisConfig());
    final ChainImporter<?> importer = new ChainImporter<>(controller);

    URL jsonDataURL = ChainImporter.class.getResource("blocks-import-valid.json");
    String jsonData = Resources.toString(jsonDataURL, UTF_8);

    importer.importChain(jsonData);

    final Blockchain blockchain = controller.getProtocolContext().getBlockchain();

    // Check blocks were imported
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(4);
    // Get imported blocks
    List<Block> blocks = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      blocks.add(getBlockAt(blockchain, i + 1));
    }

    // Run new import based on first file
    jsonDataURL = ChainImporter.class.getResource("blocks-import-valid-addendum.json");
    jsonData = Resources.toString(jsonDataURL, UTF_8);
    ObjectNode newImportData = JsonUtil.objectNodeFromString(jsonData);
    final ObjectNode block0 = (ObjectNode) newImportData.get("blocks").get(0);
    final Block parentBlock = blocks.get(3);
    block0.put("parentHash", parentBlock.getHash().toString());
    final String newImportJsonData = JsonUtil.getJson(newImportData);
    importer.importChain(newImportJsonData);

    // Check blocks were imported
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(5);
    final Block newBlock = getBlockAt(blockchain, parentBlock.getHeader().getNumber() + 1L);

    // Check block 1
    assertThat(newBlock.getHeader().getParentHash()).isEqualTo(parentBlock.getHash());
    assertThat(newBlock.getHeader().getExtraData()).isEqualTo(BytesValue.EMPTY);
    assertThat(newBlock.getHeader().getCoinbase()).isEqualTo(Address.ZERO);
    assertThat(newBlock.getBody().getTransactions().size()).isEqualTo(1);
    // Check first tx
    Transaction tx = newBlock.getBody().getTransactions().get(0);
    assertThat(tx.getSender())
        .isEqualTo(Address.fromHexString("fe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    assertThat(tx.getTo())
        .hasValue(Address.fromHexString("627306090abaB3A6e1400e9345bC60c78a8BEf57"));
    assertThat(tx.getGasLimit()).isEqualTo(0xFFFFF1L);
    assertThat(tx.getGasPrice()).isEqualTo(Wei.fromHexString("0xFF"));
    assertThat(tx.getValue()).isEqualTo(Wei.of(1L));
    assertThat(tx.getNonce()).isEqualTo(2L);
  }

  @Test
  public void importChain_invalidParent() throws IOException {
    final PantheonController<?> controller = createController(getDevGenesisConfig());
    final ChainImporter<?> importer = new ChainImporter<>(controller);

    final URL jsonDataURL =
        ChainImporter.class.getResource("blocks-import-invalid-bad-parent.json");
    final String jsonData = Resources.toString(jsonDataURL, UTF_8);

    assertThatThrownBy(() -> importer.importChain(jsonData))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Unable to locate block parent at 2456");
  }

  @Test
  public void importChain_invalidTransaction() throws IOException {
    final PantheonController<?> controller = createController(getDevGenesisConfig());
    final ChainImporter<?> importer = new ChainImporter<>(controller);

    final URL jsonDataURL = ChainImporter.class.getResource("blocks-import-invalid-bad-tx.json");
    final String jsonData = Resources.toString(jsonDataURL, UTF_8);

    assertThatThrownBy(() -> importer.importChain(jsonData))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "Unable to create block.  1 transaction(s) were found to be invalid.");
  }

  private Block getBlockAt(final Blockchain blockchain, final long blockNumber) {
    final BlockHeader header = blockchain.getBlockHeader(blockNumber).get();
    final BlockBody body = blockchain.getBlockBody(header.getHash()).get();
    return new Block(header, body);
  }

  private String getDevGenesisConfig() throws IOException {
    return Resources.toString(this.getClass().getResource("/dev.json"), UTF_8);
  }

  private PantheonController<?> createController(final String config) throws IOException {
    final Path dataDir = folder.newFolder().toPath();
    return new PantheonController.Builder()
        .fromGenesisConfig(GenesisConfigFile.fromConfig(config))
        .synchronizerConfiguration(SynchronizerConfiguration.builder().build())
        .ethProtocolConfiguration(EthProtocolConfiguration.defaultConfig())
        .storageProvider(new InMemoryStorageProvider())
        .networkId(10)
        .miningParameters(
            new MiningParametersTestBuilder()
                .minTransactionGasPrice(Wei.ZERO)
                .enabled(false)
                .build())
        .nodeKeys(KeyPair.generate())
        .metricsSystem(new NoOpMetricsSystem())
        .privacyParameters(PrivacyParameters.DEFAULT)
        .dataDirectory(dataDir)
        .clock(TestClock.fixed())
        .transactionPoolConfiguration(TransactionPoolConfiguration.builder().build())
        .build();
  }
}
