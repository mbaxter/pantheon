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
package tech.pegasys.pantheon.ethereum.mainnet;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tech.pegasys.pantheon.ethereum.mainnet.HeaderValidationMode.FULL;

import tech.pegasys.pantheon.ethereum.BlockValidator;
import tech.pegasys.pantheon.ethereum.ProtocolContext;
import tech.pegasys.pantheon.ethereum.chain.MutableBlockchain;
import tech.pegasys.pantheon.ethereum.core.Block;
import tech.pegasys.pantheon.ethereum.core.Hash;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MainnetBlockImporterTest {
  @Mock private BlockValidator<Object> blockValidator;
  @Mock private ProtocolContext<Object> context;
  @Mock private MutableBlockchain blockchain;
  @Mock private Block block;
  @Mock private Hash hash;
  private MainnetBlockImporter<Object> blockImporter;

  @Before
  public void setup() {
    blockImporter = new MainnetBlockImporter<>(blockValidator);
    when(context.getBlockchain()).thenReturn(blockchain);
    when(block.getHash()).thenReturn(hash);
  }

  @Test
  public void doNotImportBlockIfBlockchainAlreadyHasBlock() {
    when(blockchain.contains(hash)).thenReturn(true);

    assertThat(blockImporter.importBlock(context, block, FULL, FULL)).isTrue();
    verify(blockValidator, never()).validateAndProcessBlock(context, block, FULL, FULL);
    verify(blockchain, never()).appendBlock(eq(block), any());
  }
}
