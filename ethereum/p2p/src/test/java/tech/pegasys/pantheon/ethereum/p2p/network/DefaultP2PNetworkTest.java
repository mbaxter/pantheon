/*
 * Copyright 2019 ConsenSys AG.
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
package tech.pegasys.pantheon.ethereum.p2p.network;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import tech.pegasys.pantheon.crypto.SECP256K1;
import tech.pegasys.pantheon.crypto.SECP256K1.KeyPair;
import tech.pegasys.pantheon.ethereum.p2p.api.P2PNetwork;
import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.config.DiscoveryConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.NetworkingConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.config.RlpxConfiguration;
import tech.pegasys.pantheon.ethereum.p2p.rlpx.exceptions.IncompatiblePeerConnectionException;
import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.p2p.peers.Endpoint;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.Capability;
import tech.pegasys.pantheon.ethereum.p2p.wire.SubProtocol;
import tech.pegasys.pantheon.metrics.noop.NoOpMetricsSystem;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class DefaultP2PNetworkTest {

  private final Vertx vertx = Vertx.vertx();
  private final NetworkingConfiguration config =
      NetworkingConfiguration.create()
          .setDiscovery(DiscoveryConfiguration.create().setActive(false))
          .setSupportedProtocols(subProtocol())
          .setRlpx(RlpxConfiguration.create().setBindPort(0));

  @After
  public void closeVertx() {
    vertx.close();
  }

  @Test
  public void handshaking() throws Exception {
    final SECP256K1.KeyPair listenKp = SECP256K1.KeyPair.generate();
    try (final P2PNetwork listener = builder().keyPair(listenKp).build();
        final P2PNetwork connector = builder().build()) {

      final int listenPort = listener.getLocalPeerInfo().getPort();
      listener.start();
      connector.start();
      final BytesValue listenId = listenKp.getPublicKey().getEncodedBytes();
      assertThat(
              connector
                  .connect(
                      new DefaultPeer(
                          listenId,
                          new Endpoint(
                              InetAddress.getLoopbackAddress().getHostAddress(),
                              listenPort,
                              OptionalInt.of(listenPort))))
                  .get(30L, TimeUnit.SECONDS)
                  .getPeerInfo()
                  .getNodeId())
          .isEqualTo(listenId);
    }
  }

  @Test
  public void rejectPeerWithNoSharedCaps() throws Exception {
    final SECP256K1.KeyPair listenKp = SECP256K1.KeyPair.generate();

    final SubProtocol subprotocol1 = subProtocol("eth");
    final Capability cap1 = Capability.create(subprotocol1.getName(), 63);
    final SubProtocol subprotocol2 = subProtocol("oth");
    final Capability cap2 = Capability.create(subprotocol2.getName(), 63);
    try (final P2PNetwork listener =
            builder().keyPair(listenKp).supportedCapabilities(cap1).build();
        final P2PNetwork connector =
            builder().keyPair(listenKp).supportedCapabilities(cap2).build()) {
      final int listenPort = listener.getLocalPeerInfo().getPort();
      listener.start();
      connector.start();
      final BytesValue listenId = listenKp.getPublicKey().getEncodedBytes();

      final Peer listenerPeer =
          new DefaultPeer(
              listenId,
              new Endpoint(
                  InetAddress.getLoopbackAddress().getHostAddress(),
                  listenPort,
                  OptionalInt.of(listenPort)));
      final CompletableFuture<PeerConnection> connectFuture = connector.connect(listenerPeer);
      assertThatThrownBy(connectFuture::get)
          .hasCauseInstanceOf(IncompatiblePeerConnectionException.class);
    }
  }

  private static SubProtocol subProtocol() {
    return subProtocol("eth");
  }

  private static SubProtocol subProtocol(final String name) {
    return new SubProtocol() {
      @Override
      public String getName() {
        return "eth";
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
        return INVALID_MESSAGE_NAME;
      }
    };
  }

  private DefaultP2PNetwork.Builder builder() {
    return DefaultP2PNetwork.builder()
        .vertx(vertx)
        .config(config)
        .keyPair(KeyPair.generate())
        .metricsSystem(new NoOpMetricsSystem())
        .supportedCapabilities(Arrays.asList(Capability.create("eth", 63)));
  }
}
