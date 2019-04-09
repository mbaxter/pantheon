package tech.pegasys.pantheon.ethereum.p2p.exceptions;

public abstract class P2PNetworkException extends RuntimeException {
  public P2PNetworkException(final String message) {
    super(message);
  }
}
