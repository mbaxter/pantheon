package tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection;

public abstract class ConnectionException extends RuntimeException {

  public ConnectionException(String message) {
    super(message);
  }
}
