package tech.pegasys.pantheon.ethereum.p2p.rlpx;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import tech.pegasys.pantheon.ethereum.p2p.api.MessageData;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.PeerInfo;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.metrics.Counter;
import tech.pegasys.pantheon.metrics.LabelledMetric;
import tech.pegasys.pantheon.metrics.MetricCategory;
import tech.pegasys.pantheon.metrics.MetricsSystem;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.bytes.BytesValue;

public abstract class RlpxAgent {

  private final BytesValue nodeId;
  private final NetworkingConfiguration config;
  private final List<Capability> capabilities;

  protected Subscribers<ConnectCallback> connectSubscribers = new Subscribers<>();
  protected Subscribers<DisconnectCallback> disconnectSubscribers = new Subscribers<>();
  protected Map<Capability, Subscribers<MessageCallback>> messageSubscribers = new ConcurrentHashMap<>();

  private AtomicBoolean started = new AtomicBoolean();
  private AtomicBoolean stopped = new AtomicBoolean();
  private volatile OptionalInt listeningPort = OptionalInt.empty();

  private final LabelledMetric<Counter> disconnectCounter;
  private final Counter connectedPeersCounter;
  protected volatile PeerInfo ourPeerInfo;

  protected RlpxAgent(final BytesValue nodeId, final NetworkingConfiguration config, final List<Capability> capabilities, final MetricsSystem metrics) {
    this.nodeId = nodeId;
    this.config = config;
    this.capabilities = capabilities;

    // Setup metrics
    disconnectCounter =
      metrics.createLabelledCounter(
        MetricCategory.PEERS,
        "disconnected_total",
        "Total number of peers disconnected",
        "initiator",
        "disconnectReason");

    connectedPeersCounter =
      metrics.createCounter(
        MetricCategory.PEERS, "connected_total", "Total number of peers connected");

    metrics.createGauge(
      MetricCategory.PEERS,
      "peer_count_current",
      "Number of peers currently connected",
      () -> (double) getConnections().count());
  }

  public CompletableFuture<Integer> start() {
    if (started.compareAndSet(false, true)) {
      final CompletableFuture<Integer> startFuture = startListening(config.getRlpx(), capabilities);
      return startFuture.thenApply((port) -> {
        listeningPort = OptionalInt.of(port);
        ourPeerInfo =
          new PeerInfo(
            5,
            config.getClientId(),
            capabilities,
            port,
            nodeId);
        return port;
      });
    } else {
      throw new IllegalStateException("Unable to start and already started " + getClass().getSimpleName());
    }
  }

  public CompletableFuture<Void> stop() {
    if (started.get() && stopped.compareAndSet(false, true)) {
      getConnections().forEach((conn) -> conn.disconnect(DisconnectReason.CLIENT_QUITTING));
      return stopListening();
    } else {
      throw new IllegalStateException("Unable to stop a " + getClass().getSimpleName() + " that is not running");
    }
  }

  public PeerInfo getPeerInfo() {
    return ourPeerInfo;
  }

  public abstract Stream<? extends PeerConnection> getConnections();

  public CompletableFuture<PeerConnection>  connect(final Peer peer) {
    CompletableFuture<PeerConnection> future = new CompletableFuture<>();
    if (!started.get() || ourPeerInfo == null) {
      Exception err = new IllegalStateException("Cannot connect before " + this.getClass().getSimpleName() + " has finished starting");
      future.completeExceptionally(err);
    } else {
      doConnect(peer, future);
    }

    return future;
  }

  protected abstract CompletableFuture<PeerConnection> doConnect(final Peer peer, CompletableFuture<PeerConnection> connectionFuture);

  protected abstract CompletableFuture<Integer> startListening(
    final RlpxConfiguration config, final List<Capability> supportedCapabilities);

  protected abstract CompletableFuture<Void> stopListening();

  public OptionalInt getListeningPort() {
    return listeningPort;
  }

  public void subscribeMessage(Capability capability, MessageCallback callback) {
    messageSubscribers.computeIfAbsent(capability, key -> new Subscribers<>()).subscribe(callback);
  }

  public void subscribeConnect(ConnectCallback callback) {
    connectSubscribers.subscribe(callback);
  }

  public void subscribeDisconnect(DisconnectCallback callback) {
    disconnectSubscribers.subscribe(callback);
  }

  protected void dispatchConnect(final PeerConnection connection, final boolean initiatedByPeer) {
    connectedPeersCounter.inc();
    connectSubscribers.forEach(c -> c.onConnect(connection, initiatedByPeer));
  }

  protected void dispatchDisconnect(
    final PeerConnection connection, final DisconnectReason reason, final boolean initiatedByPeer) {
    disconnectCounter.labels(initiatedByPeer ? "remote" : "local", reason.name()).inc();
    disconnectSubscribers.forEach(s -> s.onDisconnect(connection, reason, initiatedByPeer));
  }

  protected void dispatchMessage(
    final PeerConnection connection, final Capability capability, final MessageData message) {
    messageSubscribers.getOrDefault(capability, Subscribers.none()).forEach(s -> s.onMessage(connection, capability, message));
  }

}
