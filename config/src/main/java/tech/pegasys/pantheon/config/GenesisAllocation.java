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
package tech.pegasys.pantheon.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GenesisAllocation {
  private final String address;
  private final ObjectNode data;

  GenesisAllocation(final String address, final ObjectNode data) {
    this.address = address;
    this.data = data;
  }

  public String getAddress() {
    return address;
  }

  public String getBalance() {
    return JsonUtil.getValue(data, "balance", JsonNode::asText, "0");
  }

  public String getCode() {
    return JsonUtil.getValue(data, "code", JsonNode::asText, "");
  }

  public String getNonce() {
    return JsonUtil.getValue(data, "nonce", JsonNode::asText, "0");
  }

  public String getVersion() {
    return JsonUtil.getValue(data, "version", JsonNode::asText, "");
  }

  public ObjectNode getStorage() {
    return JsonUtil.getObjectNode(data, "storage").orElse(JsonUtil.createEmptyObjectNode());
  }
}
