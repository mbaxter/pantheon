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
package tech.pegasys.pantheon.ethereum.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import tech.pegasys.pantheon.ethereum.core.Hash;
import tech.pegasys.pantheon.ethereum.storage.keyvalue.KeyValueStorageWorldStateStorage;
import tech.pegasys.pantheon.ethereum.trie.MerklePatriciaTrie;
import tech.pegasys.pantheon.services.kvstore.InMemoryKeyValueStorage;
import tech.pegasys.pantheon.util.bytes.BytesValue;

import org.junit.Test;

public class WorldStateArchiveTest {

  @Test
  public void getNodeData_whenEmptyReturnsRequestForEmptyValue() {
    WorldStateArchive archive = emptyArchive();
    assertThat(archive.getNodeData(Hash.EMPTY)).contains(BytesValue.EMPTY);
  }

  @Test
  public void getNodeData_whenEmptyReturnsRequestForEmptyTrieNode() {
    WorldStateArchive archive = emptyArchive();
    assertThat(archive.getNodeData(Hash.wrap(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)))
        .contains(MerklePatriciaTrie.EMPTY_TRIE_NODE);
  }

  private WorldStateArchive emptyArchive() {
    WorldStateStorage storage = new KeyValueStorageWorldStateStorage(new InMemoryKeyValueStorage());
    return new WorldStateArchive(storage);
  }
}
