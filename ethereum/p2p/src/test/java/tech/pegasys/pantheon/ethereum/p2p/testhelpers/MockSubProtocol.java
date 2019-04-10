package tech.pegasys.pantheon.ethereum.p2p.testhelpers;

import tech.pegasys.pantheon.ethereum.p2p.wire.SubProtocol;

public class MockSubProtocol implements SubProtocol {
  private final String name;

  private MockSubProtocol(final String name) {
    this.name = name;
  }

  public static MockSubProtocol create() {
    return new MockSubProtocol("eth");
  }

  public static MockSubProtocol create(String name) {
    return new MockSubProtocol(name);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int messageSpace(final int protocolVersion) {
    return 8;
  }

  @Override
  public boolean isValidMessageCode(final int protocolVersion, final int code) {
    return true;
  }

  @Override
  public String messageName(final int protocolVersion, final int code) {
    return "TestMessageName";
  }
}
