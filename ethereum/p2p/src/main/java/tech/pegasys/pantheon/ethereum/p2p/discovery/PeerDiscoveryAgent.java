/*
 * Copyright 2018 ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package tech.pegasys.pantheon.ethereum.p2p.discovery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static tech.pegasys.pantheon.util.Preconditions.checkGuard;
import static tech.pegasys.pantheon.util.bytes.BytesValue.wrapBuffer;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.ethereum.p2p.api.DisconnectCallback;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.DiscoveryConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.discovery.PeerDiscoveryEvent.PeerBondedEvent;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.Packet;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.PeerDiscoveryController;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.PeerRequirement;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.PeerTable;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.PingPacketData;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.VertxTimerUtil;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeerId;
import tech.pegasys.pantheon.ethereum.p2p.peers.PeerBlacklist;
import tech.pegasys.pantheon.ethereum.p2p.permissioning.NodeWhitelistController;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage;
import tech.pegasys.pantheon.util.NetworkUtility;
import tech.pegasys.pantheon.util.Subscribers;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.vertx.core.Vertx;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The peer discovery agent is the network component that sends and receives messages peer discovery
 * messages via UDP.
 */
public class PeerDiscoveryAgent implements DisconnectCallback {
  private static final Logger LOG = LogManager.getLogger();

  // The devp2p specification says only accept packets up to 1280, but some
  // clients ignore that, so we add in a little extra padding.
  private static final int MAX_PACKET_SIZE_BYTES = 1600;
  protected static final long PEER_REFRESH_INTERVAL_MS = MILLISECONDS.convert(30, TimeUnit.MINUTES);
  private final Vertx vertx;

  protected final List<DiscoveryPeer> bootstrapPeers;
  protected final PeerRequirement peerRequirement;
  protected final PeerBlacklist peerBlacklist;
  protected final NodeWhitelistController nodeWhitelistController;
  /* The peer controller, which takes care of the state machine of peers. */
  private Optional<PeerDiscoveryController> controller = Optional.empty();

  /* The keypair used to sign messages. */
  protected final SECP256K1.KeyPair keyPair;
  protected final PeerTable peerTable;
  protected final DiscoveryConfiguration config;

  /* This is the {@link tech.pegasys.pantheon.ethereum.p2p.Peer} object holding who we are. */
  protected DiscoveryPeer advertisedPeer;
  /* The vert.x UDP socket. */
  private DatagramSocket socket;

  /* Is discovery enabled? */
  private boolean isActive = false;
  protected final Subscribers<Consumer<PeerBondedEvent>> peerBondedObservers = new Subscribers<>();

  public PeerDiscoveryAgent(
      final Vertx vertx,
      final SECP256K1.KeyPair keyPair,
      final DiscoveryConfiguration config,
      final PeerRequirement peerRequirement,
      final PeerBlacklist peerBlacklist,
      final NodeWhitelistController nodeWhitelistController) {
    checkArgument(vertx != null, "vertx instance cannot be null");
    checkArgument(keyPair != null, "keypair cannot be null");
    checkArgument(config != null, "provided configuration cannot be null");

    validateConfiguration(config);

    this.peerRequirement = peerRequirement;
    this.peerBlacklist = peerBlacklist;
    this.nodeWhitelistController = nodeWhitelistController;
    this.bootstrapPeers =
        config.getBootstrapPeers().stream().map(DiscoveryPeer::new).collect(Collectors.toList());

    this.vertx = vertx;
    this.config = config;
    this.keyPair = keyPair;
    this.peerTable = new PeerTable(keyPair.getPublicKey().getEncodedBytes(), 16);
  }

  public CompletableFuture<?> start() {
    final CompletableFuture<?> future = new CompletableFuture<>();
    if (config.isActive()) {
      final String host = config.getBindHost();
      final int port = config.getBindPort();
      LOG.info("Starting peer discovery agent on host={}, port={}", host, port);

      listenForConnections()
          .thenAccept(
              (Integer effectivePort) -> {
                // Once listener is set up, finish intitializing
                final BytesValue id = keyPair.getPublicKey().getEncodedBytes();
                advertisedPeer =
                    new DiscoveryPeer(id, config.getAdvertisedHost(), effectivePort, effectivePort);
                isActive = true;
                startController();
              })
          .whenComplete(
              (res, err) -> {
                // Finalize future
                if (err != null) {
                  future.completeExceptionally(err);
                } else {
                  future.complete(null);
                }
              });
    } else {
      this.isActive = false;
      future.complete(null);
    }
    return future;
  }

  protected void startController() {
    PeerDiscoveryController controller = createController();
    this.controller = Optional.of(controller);
    controller.start();
  }

  @VisibleForTesting
  protected PeerDiscoveryController createController() {
    return new PeerDiscoveryController(
        new VertxTimerUtil(vertx),
        keyPair,
        advertisedPeer,
        peerTable,
        bootstrapPeers,
        PEER_REFRESH_INTERVAL_MS,
        peerRequirement,
        peerBlacklist,
        nodeWhitelistController,
        this::sendPacket,
        peerBondedObservers);
  }

  protected CompletableFuture<Integer> listenForConnections() {
    CompletableFuture<Integer> future = new CompletableFuture<>();
    vertx
        .createDatagramSocket(new DatagramSocketOptions().setIpV6(NetworkUtility.isIPv6Available()))
        .listen(
            config.getBindPort(),
            config.getBindHost(),
            res -> {
              if (res.failed()) {
                Throwable cause = res.cause();
                LOG.error("An exception occurred when starting the peer discovery agent", cause);

                if (cause instanceof BindException || cause instanceof SocketException) {
                  cause =
                      new PeerDiscoveryServiceException(
                          String.format(
                              "Failed to bind Ethereum UDP discovery listener to %s:%d: %s",
                              config.getBindHost(), config.getBindPort(), cause.getMessage()));
                }
                future.completeExceptionally(cause);
                return;
              }

              this.socket = res.result();

              // TODO: when using wildcard hosts (0.0.0.0), we need to handle multiple addresses by
              // selecting
              // the correct 'announce' address.
              final String effectiveHost = socket.localAddress().host();
              final int effectivePort = socket.localAddress().port();

              LOG.info(
                  "Started peer discovery agent successfully, on effective host={} and port={}",
                  effectiveHost,
                  effectivePort);

              socket.exceptionHandler(this::handleException);
              socket.handler(this::handlePacket);

              future.complete(effectivePort);
            });
    return future;
  }

  public CompletableFuture<?> stop() {
    if (socket == null) {
      return CompletableFuture.completedFuture(null);
    }

    final CompletableFuture<?> completion = new CompletableFuture<>();
    socket.close(
        ar -> {
          if (ar.succeeded()) {
            controller.ifPresent(PeerDiscoveryController::stop);
            socket = null;
            completion.complete(null);
          } else {
            completion.completeExceptionally(ar.cause());
          }
        });
    return completion;
  }

  /**
   * For uncontrolled exceptions occurring in the packet handlers.
   *
   * @param exception the exception that was raised
   */
  private void handleException(final Throwable exception) {
    if (exception instanceof IOException) {
      LOG.debug("Packet handler exception", exception);
    } else {
      LOG.error("Packet handler exception", exception);
    }
  }

  /**
   * The UDP packet handler. This is the entrypoint for all received datagrams.
   *
   * @param datagram the received datagram.
   */
  private void handlePacket(final DatagramPacket datagram) {
    try {
      final int length = datagram.data().length();
      checkGuard(
          length <= MAX_PACKET_SIZE_BYTES,
          PeerDiscoveryPacketDecodingException::new,
          "Packet too large. Actual size (bytes): %s",
          length);

      // We allow exceptions to bubble up, as they'll be picked up by the exception handler.
      final Packet packet = Packet.decode(datagram.data());

      OptionalInt fromPort = OptionalInt.empty();
      if (packet.getPacketData(PingPacketData.class).isPresent()) {
        final PingPacketData ping = packet.getPacketData(PingPacketData.class).orElseGet(null);
        if (ping != null && ping.getFrom() != null && ping.getFrom().getTcpPort().isPresent()) {
          fromPort = ping.getFrom().getTcpPort();
        }
      }

      // Acquire the senders coordinates to build a Peer representation from them.
      final String addr = datagram.sender().host();
      final int port = datagram.sender().port();

      // Notify the peer controller.
      final DiscoveryPeer peer = new DiscoveryPeer(packet.getNodeId(), addr, port, fromPort);
      controller.ifPresent(c -> c.onMessage(packet, peer));
    } catch (final PeerDiscoveryPacketDecodingException e) {
      LOG.debug("Discarding invalid peer discovery packet", e);
    } catch (final Throwable t) {
      LOG.error("Encountered error while handling packet", t);
    }
  }

  /**
   * Send a packet to the given recipient.
   *
   * @param peer the recipient
   * @param packet the packet to send
   */
  public void sendPacket(final DiscoveryPeer peer, final Packet packet) {
    LOG.trace(
        ">>> Sending {} discovery packet to {} ({}): {}",
        packet.getType(),
        peer.getEndpoint(),
        peer.getId().slice(0, 16),
        packet);

    // Update the lastContacted timestamp on the peer if the dispatch succeeds.
    socket.send(
        packet.encode(),
        peer.getEndpoint().getUdpPort(),
        peer.getEndpoint().getHost(),
        ar -> {
          if (ar.failed()) {
            LOG.warn(
                "Sending to peer {} failed, packet: {}",
                peer,
                wrapBuffer(packet.encode()),
                ar.cause());
            return;
          }
          if (ar.succeeded()) {
            peer.setLastContacted(System.currentTimeMillis());
          }
        });
  }

  @VisibleForTesting
  public Collection<DiscoveryPeer> getPeers() {
    return controller
        .map(PeerDiscoveryController::getPeers)
        .map(Collections::unmodifiableCollection)
        .orElse(Collections.emptyList());
  }

  public DiscoveryPeer getAdvertisedPeer() {
    return advertisedPeer;
  }

  public InetSocketAddress localAddress() {
    checkState(socket != null, "uninitialized discovery agent");
    return new InetSocketAddress(socket.localAddress().host(), socket.localAddress().port());
  }

  /**
   * Adds an observer that will get called when a new peer is bonded with and added to the peer
   * table.
   *
   * <p><i>No guarantees are made about the order in which observers are invoked.</i>
   *
   * @param observer The observer to call.
   * @return A unique ID identifying this observer, to that it can be removed later.
   */
  public long observePeerBondedEvents(final Consumer<PeerBondedEvent> observer) {
    checkNotNull(observer);
    return peerBondedObservers.subscribe(observer);
  }

  /**
   * Removes an previously added peer bonded observer.
   *
   * @param observerId The unique ID identifying the observer to remove.
   * @return Whether the observer was located and removed.
   */
  public boolean removePeerBondedObserver(final long observerId) {
    return peerBondedObservers.unsubscribe(observerId);
  }

  /**
   * Returns the count of observers that are registered on this controller.
   *
   * @return The observer count.
   */
  @VisibleForTesting
  public int getObserverCount() {
    return peerBondedObservers.getSubscriberCount();
  }

  private static void validateConfiguration(final DiscoveryConfiguration config) {
    checkArgument(
        config.getBindHost() != null && InetAddresses.isInetAddress(config.getBindHost()),
        "valid bind host required");
    checkArgument(
        config.getAdvertisedHost() != null
            && InetAddresses.isInetAddress(config.getAdvertisedHost()),
        "valid advertisement host required");
    checkArgument(
        config.getBindPort() == 0 || NetworkUtility.isValidPort(config.getBindPort()),
        "valid port number required");
    checkArgument(config.getBootstrapPeers() != null, "bootstrapPeers cannot be null");
    checkArgument(config.getBucketSize() > 0, "bucket size cannot be negative nor zero");
  }

  @Override
  public void onDisconnect(
      final PeerConnection connection,
      final DisconnectMessage.DisconnectReason reason,
      final boolean initiatedByPeer) {
    final BytesValue nodeId = connection.getPeer().getNodeId();
    peerTable.evict(new DefaultPeerId(nodeId));
  }

  /**
   * Returns the current state of the PeerDiscoveryAgent.
   *
   * <p>If true, the node is actively listening for new connections. If false, discovery has been
   * turned off and the node is not listening for connections.
   *
   * @return true, if the {@link PeerDiscoveryAgent} is active on this node, false, otherwise.
   */
  public boolean isActive() {
    return isActive;
  }
}
