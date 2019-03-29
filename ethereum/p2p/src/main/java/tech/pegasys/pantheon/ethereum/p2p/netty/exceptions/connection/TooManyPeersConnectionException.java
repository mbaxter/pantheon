package tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection;

public class TooManyPeersConnectionException extends ConnectionException {
  public TooManyPeersConnectionException(String message) {
    super(message);
  }

  public TooManyPeersConnectionException() {
    this("Too many peers");
  }
}
