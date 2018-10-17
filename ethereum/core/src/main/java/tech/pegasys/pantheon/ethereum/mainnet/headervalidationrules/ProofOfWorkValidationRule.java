package tech.pegasys.pantheon.ethereum.mainnet.headervalidationrules;

import tech.pegasys.pantheon.crypto.BouncyCastleMessageDigestFactory;
import tech.pegasys.pantheon.ethereum.core.BlockHeader;
import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.mainnet.DetachedBlockHeaderValidationRule;
import tech.pegasys.pantheon.ethereum.mainnet.EthHasher;
import tech.pegasys.pantheon.ethereum.rlp.BytesValueRLPOutput;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.bytes.BytesValues;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ProofOfWorkValidationRule implements DetachedBlockHeaderValidationRule {

  private static final Logger LOG = LogManager.getLogger(ProofOfWorkValidationRule.class);

  private static final int SERIALIZED_HASH_SIZE = 33;

  private static final int SERIALIZED_NONCE_SIZE = 9;

  private static final BigInteger ETHHASH_TARGET_UPPER_BOUND = BigInteger.valueOf(2).pow(256);

  private static final EthHasher HASHER = new EthHasher.Light();

  private static final ThreadLocal<MessageDigest> KECCAK_256 =
      ThreadLocal.withInitial(
          () -> {
            try {
              return BouncyCastleMessageDigestFactory.create(
                  tech.pegasys.pantheon.crypto.Hash.KECCAK256_ALG);
            } catch (final NoSuchAlgorithmException ex) {
              throw new IllegalStateException(ex);
            }
          });

  @Override
  public boolean validate(final BlockHeader header, final BlockHeader parent) {
    final byte[] hashBuffer = new byte[64];
    final Hash headerHash = hashHeader(header);
    HASHER.hash(hashBuffer, header.getNonce(), header.getNumber(), headerHash.extractArray());

    if (header.getDifficulty().isZero()) {
      LOG.trace("Rejecting header because difficulty is 0");
      return false;
    }
    final BigInteger difficulty =
        BytesValues.asUnsignedBigInteger(header.getDifficulty().getBytes());
    final UInt256 target = UInt256.of(ETHHASH_TARGET_UPPER_BOUND.divide(difficulty));

    final UInt256 result = UInt256.wrap(Bytes32.wrap(hashBuffer, 32));
    if (result.compareTo(target) > 0) {
      LOG.warn(
          "Invalid block header: the EthHash result {} was greater than the target {}.\n"
              + "Failing header:\n{}",
          result,
          target,
          header);
      return false;
    }

    final Hash mixedHash =
        Hash.wrap(Bytes32.leftPad(BytesValue.wrap(hashBuffer).slice(0, Bytes32.SIZE)));
    if (!header.getMixHash().equals(mixedHash)) {
      LOG.warn(
          "Invalid block header: header mixed hash {} does not equal calculated mixed hash {}.\n"
              + "Failing header:\n{}",
          header.getMixHash(),
          mixedHash,
          header);
      return false;
    }

    return true;
  }

  private Hash hashHeader(BlockHeader header) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();

    // Encode header without nonce and mixhash
    out.startList();
    out.writeBytesValue(header.getParentHash());
    out.writeBytesValue(header.getOmmersHash());
    out.writeBytesValue(header.getCoinbase());
    out.writeBytesValue(header.getStateRoot());
    out.writeBytesValue(header.getTransactionsRoot());
    out.writeBytesValue(header.getReceiptsRoot());
    out.writeBytesValue(header.getLogsBloom().getBytes());
    out.writeUInt256Scalar(header.getDifficulty());
    out.writeLongScalar(header.getNumber());
    out.writeLongScalar(header.getGasLimit());
    out.writeLongScalar(header.getGasUsed());
    out.writeLongScalar(header.getTimestamp());
    out.writeBytesValue(header.getExtraData());
    out.endList();

    return Hash.hash(out.encoded());
  }

  @Override
  public boolean includeInLightValidation() {
    return false;
  }

  private static int writeListPrefix(final int size, final byte[] target) {
    final int sizeLength = 4 - Integer.numberOfLeadingZeros(size) / 8;
    target[0] = (byte) (0xf7 + sizeLength);
    int shift = 0;
    for (int i = 0; i < sizeLength; i++) {
      target[sizeLength - i] = (byte) (size >> shift);
      shift += 8;
    }
    return 1 + sizeLength;
  }
}
