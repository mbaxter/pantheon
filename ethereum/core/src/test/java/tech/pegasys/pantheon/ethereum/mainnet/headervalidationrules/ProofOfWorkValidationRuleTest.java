package tech.pegasys.pantheon.ethereum.mainnet.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;

import tech.pegasys.pantheon.ethereum.core.BlockHashFunction;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.BlockHeaderBuilder;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.mainnet.MainnetProtocolSchedule;
import tech.pegasys.pantheon.ethereum.mainnet.ProtocolSchedule;
import tech.pegasys.pantheon.ethereum.mainnet.ScheduleBasedBlockHashFunction;
import tech.pegasys.pantheon.ethereum.mainnet.ValidationTestUtils;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ProofOfWorkValidationRuleTest {

  private final BlockHeader blockHeader;
  private final BlockHeader parentHeader;
  private final ProofOfWorkValidationRule validationRule;

  public ProofOfWorkValidationRuleTest(long parentBlockNum, long blockNum) throws IOException {
    blockHeader = ValidationTestUtils.readHeader(parentBlockNum);
    parentHeader = ValidationTestUtils.readHeader(blockNum);
    validationRule = new ProofOfWorkValidationRule();
  }

  @Parameters(name = "block {1}")
  public static Collection<Object[]> data() {

    return Arrays.asList(
        new Object[][] {
          {300005, 300006},
          {1200000, 1200001},
          {4400000, 4400001},
          {4400001, 4400002}
        });
  }

  @Test
  public void validatesValidBlocks() {
    assertThat(validationRule.validate(blockHeader, parentHeader)).isTrue();
  }

  @Test
  public void failsBlockWithZeroValuedDifficulty() {
    BlockHeader header =
        BlockHeaderBuilder.fromHeader(blockHeader)
            .difficulty(UInt256.ZERO)
            .blockHashFunction(mainnetBlockHashFunction())
            .buildBlockHeader();
    assertThat(validationRule.validate(header, parentHeader)).isFalse();
  }

  @Test
  public void failsWithVeryLargeDifficulty() {
    UInt256 largeDifficulty = UInt256.of(BigInteger.valueOf(2).pow(255));
    BlockHeader header =
        BlockHeaderBuilder.fromHeader(blockHeader)
            .difficulty(largeDifficulty)
            .blockHashFunction(mainnetBlockHashFunction())
            .buildBlockHeader();
    assertThat(validationRule.validate(header, parentHeader)).isFalse();
  }

  @Test
  public void failsWithMisMatchedMixHash() {
    Hash updateMixHash = Hash.wrap(blockHeader.getMixHash().asUInt256().minus(1L).getBytes());
    BlockHeader header =
        BlockHeaderBuilder.fromHeader(blockHeader)
            .mixHash(updateMixHash)
            .blockHashFunction(mainnetBlockHashFunction())
            .buildBlockHeader();
    assertThat(validationRule.validate(header, parentHeader)).isFalse();
  }

  @Test
  public void failsWithMisMatchedNonce() {
    long updatedNonce = blockHeader.getNonce() + 1;
    BlockHeader header =
        BlockHeaderBuilder.fromHeader(blockHeader)
            .nonce(updatedNonce)
            .blockHashFunction(mainnetBlockHashFunction())
            .buildBlockHeader();
    assertThat(validationRule.validate(header, parentHeader)).isFalse();
  }

  private BlockHashFunction mainnetBlockHashFunction() {
    ProtocolSchedule<Void> protocolSchedule = MainnetProtocolSchedule.create();
    return ScheduleBasedBlockHashFunction.create(protocolSchedule);
  }
}
