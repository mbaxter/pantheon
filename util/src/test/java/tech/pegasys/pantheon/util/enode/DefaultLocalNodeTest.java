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
package tech.pegasys.pantheon.util.enode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import tech.pegasys.pantheon.util.bytes.BytesValue;
import tech.pegasys.pantheon.util.enode.LocalNode.NodeNotReadyException;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

public class DefaultLocalNodeTest {
  private final EnodeURL enode =
      EnodeURL.builder()
          .ipAddress("127.0.0.1")
          .listeningPort(30303)
          .nodeId(BytesValue.of(new byte[64]))
          .build();

  @Test
  public void create() {
    final LocalNode localNode = DefaultLocalNode.create();
    assertThat(localNode.isReady()).isFalse();
    assertThatThrownBy(localNode::getEnode).isInstanceOf(NodeNotReadyException.class);
  }

  @Test
  public void setEnode() {
    final MutableLocalNode localNode = DefaultLocalNode.create();
    localNode.setEnode(enode);

    assertThat(localNode.isReady()).isTrue();
    final EnodeURL enodeValue = localNode.getEnode();
    assertThat(enodeValue).isEqualTo(enode);
  }

  @Test
  public void subscribeReady_beforeReady() {
    AtomicReference<EnodeURL> localEnode = new AtomicReference<>(null);
    final MutableLocalNode localNode = DefaultLocalNode.create();
    localNode.subscribeReady(localEnode::set);

    assertThat(localEnode.get()).isNull();

    localNode.setEnode(enode);
    assertThat(localEnode.get()).isEqualTo(enode);
  }

  @Test
  public void subscribeReady_afterReady() {
    AtomicReference<EnodeURL> localEnode = new AtomicReference<>(null);
    final MutableLocalNode localNode = DefaultLocalNode.create();
    localNode.setEnode(enode);

    localNode.subscribeReady(localEnode::set);
    assertThat(localEnode.get()).isEqualTo(enode);
  }

  @Test
  public void subscribeReady_beforeAndAfterReady() {
    final MutableLocalNode localNode = DefaultLocalNode.create();

    AtomicReference<EnodeURL> subscriberA = new AtomicReference<>(null);
    AtomicReference<EnodeURL> subscriberB = new AtomicReference<>(null);

    localNode.subscribeReady(subscriberA::set);
    assertThat(subscriberA.get()).isNull();

    localNode.setEnode(enode);

    localNode.subscribeReady(subscriberB::set);
    assertThat(subscriberA.get()).isEqualTo(enode);
    assertThat(subscriberB.get()).isEqualTo(enode);
  }
}
