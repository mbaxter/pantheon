package tech.pegasys.pantheon.ethereum.eth.sync.worldstate;

public enum RequestType {
  ACCOUNT_TRIE_NODE((byte) 1),
  STORAGE_TRIE_NODE((byte) 2),
  CODE((byte) 3);

  private final byte value;
  RequestType(final byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }

  public static RequestType fromValue(byte value) {
    switch (value) {
      case (byte) 1:
        return ACCOUNT_TRIE_NODE;
      case (byte) 2:
        return STORAGE_TRIE_NODE;
      case (byte) 3:
        return CODE;
      default:
        throw new IllegalArgumentException("Invalid value supplied");
    }
  }
}
