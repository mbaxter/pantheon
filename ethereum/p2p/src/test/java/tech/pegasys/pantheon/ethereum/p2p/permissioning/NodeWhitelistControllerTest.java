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
package tech.pegasys.pantheon.ethereum.p2p.permissioning;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.pegasys.pantheon.ethereum.p2p.permissioning.NodeWhitelistController.NodesWhitelistResult;
import static tech.pegasys.pantheon.ethereum.p2p.permissioning.NodeWhitelistController.NodesWhitelistResultType;

import tech.pegasys.pantheon.ethereum.p2p.peers.DefaultPeer;
import tech.pegasys.pantheon.ethereum.permissioning.PermissioningConfiguration;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NodeWhitelistControllerTest {

  private NodeWhitelistController controller;

  private final String enode1 =
      "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.10:4567";
  private final String enode2 =
      "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.10:4567";

  @Before
  public void setUp() {
    controller = new NodeWhitelistController(new PermissioningConfiguration());
  }

  @Test
  public void shouldNotAddDuplicateNodes() {
    controller.addNode(DefaultPeer.fromURI(enode1));

    NodesWhitelistResult expected =
        new NodesWhitelistResult(NodesWhitelistResultType.ADD_ERROR_DUPLICATED_ENTRY);
    NodesWhitelistResult actualResult =
        controller.addNodes(
            Lists.newArrayList(DefaultPeer.fromURI(enode1), DefaultPeer.fromURI(enode2)));

    assertThat(actualResult).isEqualToComparingOnlyGivenFields(expected, "result");
  }

  @Test
  public void shouldNotRemoveNodesThatDoNotExist() {
    NodesWhitelistResult expected =
        new NodesWhitelistResult(NodesWhitelistResultType.REMOVE_ERROR_ABSENT_ENTRY);
    NodesWhitelistResult actualResult =
        controller.removeNodes(
            Lists.newArrayList(DefaultPeer.fromURI(enode1), DefaultPeer.fromURI(enode2)));

    assertThat(actualResult).isEqualToComparingOnlyGivenFields(expected, "result");
  }
}
