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
package tech.pegasys.pantheon.ethereum.p2p.rlpx.connections;

import static com.google.common.base.Preconditions.checkState;

import tech.pegasys.pantheon.ethereum.p2p.api.PeerConnection;
import tech.pegasys.pantheon.ethereum.p2p.peers.Peer;
import tech.pegasys.pantheon.ethereum.p2p.wire.messages.DisconnectMessage.DisconnectReason;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public abstract class RlpxConnection {

  enum State {
    INITIALIZING_OUTBOUND,
    INITIALIZING_INBOUND,
    ESTABLISHED,
  }

  protected final AtomicReference<State> state;
  private final long initiatedAt;
  private final CompletableFuture<PeerConnection> future;

  private RlpxConnection(final State initialState, CompletableFuture<PeerConnection> future) {
    checkState(isInitializing(initialState), "Invalid initial state: " + initialState.name());
    this.state = new AtomicReference<>(initialState);
    this.future = future;
    this.initiatedAt = System.currentTimeMillis();
  }

  public static RlpxConnection inboundConnection(final PeerConnection peerConnection) {
    return new RemotelyInitiatedRlpxConnection(peerConnection);
  }

  public static RlpxConnection outboundConnection(
      final Peer peer, final CompletableFuture<PeerConnection> future) {
    return new LocallyInitiatedRlpxConnection(peer, future);
  }

  private boolean isInitializing(final State state) {
    return state == State.INITIALIZING_OUTBOUND || state == State.INITIALIZING_INBOUND;
  }

  public abstract Peer getPeer();

  public abstract void disconnect(DisconnectReason reason);

  public boolean isDisconnected() {
    return isEstablished() && getPeerConnection().isDisconnected();
  }

  public boolean failed() {
    return getFuture().isCompletedExceptionally();
  }

  public BytesValue getId() {
    return getPeer().getId();
  }

  public State getState() {
    return state.get();
  }

  public abstract PeerConnection getPeerConnection() throws ConnectionNotEstablishedException;

  public CompletableFuture<PeerConnection> getFuture() {
    return future;
  }

  public boolean isEstablished() {
    return state.get() == State.ESTABLISHED;
  }

  public abstract boolean initiatedRemotely();

  public boolean initiatedLocally() {
    return !initiatedRemotely();
  }

  public long getInitiatedAt() {
    return initiatedAt;
  }

  private static class RemotelyInitiatedRlpxConnection extends RlpxConnection {

    private final PeerConnection peerConnection;

    private RemotelyInitiatedRlpxConnection(final PeerConnection peerConnection) {
      super(State.INITIALIZING_INBOUND, CompletableFuture.completedFuture(peerConnection));
      this.peerConnection = peerConnection;
    }

    @Override
    public Peer getPeer() {
      return peerConnection.getPeer();
    }

    @Override
    public void disconnect(final DisconnectReason reason) {
      peerConnection.disconnect(reason);
    }

    @Override
    public PeerConnection getPeerConnection() {
      return peerConnection;
    }

    @Override
    public boolean initiatedRemotely() {
      return true;
    }

    @Override
    public boolean equals(final Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof RemotelyInitiatedRlpxConnection)) {
        return false;
      }
      final RemotelyInitiatedRlpxConnection that = (RemotelyInitiatedRlpxConnection) o;
      return Objects.equals(peerConnection, that.peerConnection);
    }

    @Override
    public int hashCode() {
      return Objects.hash(peerConnection);
    }
  }

  private static class LocallyInitiatedRlpxConnection extends RlpxConnection {

    private final Peer peer;
    private final CompletableFuture<PeerConnection> future;

    private LocallyInitiatedRlpxConnection(
        final Peer peer, final CompletableFuture<PeerConnection> future) {
      super(State.INITIALIZING_OUTBOUND, future);
      this.peer = peer;
      this.future = future;
    }

    @Override
    public Peer getPeer() {
      return peer;
    }

    @Override
    public void disconnect(final DisconnectReason reason) {
      future.thenAccept((conn) -> conn.disconnect(reason));
    }

    @Override
    public PeerConnection getPeerConnection() throws ConnectionNotEstablishedException {
      if (!future.isDone() || future.isCompletedExceptionally()) {
        throw new ConnectionNotEstablishedException(
            "Cannot access PeerConnection before connection is fully established.");
      }
      return future.getNow(null);
    }

    @Override
    public boolean initiatedRemotely() {
      return false;
    }

    @Override
    public boolean equals(final Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof LocallyInitiatedRlpxConnection)) {
        return false;
      }
      final LocallyInitiatedRlpxConnection that = (LocallyInitiatedRlpxConnection) o;
      return Objects.equals(peer, that.peer) && Objects.equals(future, that.future);
    }

    @Override
    public int hashCode() {
      return Objects.hash(peer, future);
    }
  }

  public static class ConnectionNotEstablishedException extends IllegalStateException {

    public ConnectionNotEstablishedException(final String message) {
      super(message);
    }
  }
}
