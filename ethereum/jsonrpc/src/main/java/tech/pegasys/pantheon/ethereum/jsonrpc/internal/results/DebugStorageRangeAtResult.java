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
package tech.pegasys.pantheon.ethereum.jsonrpc.internal.results;

import tech.pegasys.pantheon.ethereum.core.AccountStorageEntry;
import tech.pegasys.pantheon.util.bytes.Bytes32;
import tech.pegasys.pantheon.util.uint.UInt256;

import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DebugStorageRangeAtResult implements JsonRpcResult {

  public static ObjectNode create(
      final Collection<AccountStorageEntry> entries, final Bytes32 nextKey) {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode result = mapper.createObjectNode();
    final ObjectNode storageNode = mapper.createObjectNode();
    entries.forEach(
        entry -> {
          final ObjectNode storageEntry = mapper.createObjectNode();
          storageEntry.put("key", entry.getKey().map(UInt256::toHexString).orElse(null));
          storageEntry.put("value", entry.getValue().toHexString());
          storageNode.set(entry.getKeyHash().toString(), storageEntry);
        });

    result.set("storage", storageNode);
    result.put("nextKey", nextKey == null ? null : nextKey.toString());

    return result;
  }
}
