package tech.pegasys.pantheon.ethereum.p2p.testhelpers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.RlpxAgent;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.SubProtocol;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;

public class MockRlpxAgent extends RlpxAgent {
  private List<PeerConnection> connections = new ArrayList<>();

  MockRlpxAgent(final BytesValue nodeId,
    final NetworkingConfiguration config,
    final List<Capability> capabilities) {
    super(nodeId, config, capabilities, new NoOpMetricsSystem());
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public Stream<? extends PeerConnection> getConnections() {
    return connections.stream();
  }

  @Override
  protected CompletableFuture<PeerConnection> doConnect(final Peer peer,
    final CompletableFuture<PeerConnection> connectionFuture) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CompletableFuture<Integer> startListening(final RlpxConfiguration config,
    final List<Capability> supportedCapabilities) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CompletableFuture<Void> stopListening() {
    throw new UnsupportedOperationException();
  }

  public static class Builder {
    private BytesValue nodeId = Peer.randomId();
    private NetworkingConfiguration config = NetworkingConfiguration.create();
    private SubProtocol subProtocol = MockSubProtocol.create("eth");
    private List<Capability> capabilities = Arrays.asList(Capability.create("eth", 63));

    public MockRlpxAgent build() {
      config.setSupportedProtocols(subProtocol);
      return new MockRlpxAgent(nodeId, config, capabilities);
    }

  }
}
