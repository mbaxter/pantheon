package tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection;

import tech.pegasys.pantheon.ethereum.p2p.netty.exceptions.connection.ConnectionException;

public class DuplicatePeerConnectionException extends ConnectionException {
  public DuplicatePeerConnectionException(String message) {
    super(message);
  }
}
