package tech.pegasys.pantheon.ethereum.p2p.exceptions;

public class P2PNetworkNotReadyException extends P2PNetworkException {

  public P2PNetworkNotReadyException(final String message) {
    super(message);
  }

  public P2PNetworkNotReadyException() {
    this("Network not ready");
  }
}
