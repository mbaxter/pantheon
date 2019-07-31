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
package tech.pegasys.pantheon.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

public class JsonUtilTest {
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void getOptionalLong_nonExistentKey() {
    final ObjectNode node = mapper.createObjectNode();
    final OptionalLong result = JsonUtil.getOptionalLong(node, "test");
    assertThat(result).isEmpty();
  }

  @Test
  public void getOptionalLong_nullValue() {
    final ObjectNode node = mapper.createObjectNode();
    node.set("test", null);
    final OptionalLong result = JsonUtil.getOptionalLong(node, "test");
    assertThat(result).isEmpty();
  }

  @Test
  public void getOptionalLong_validValue() {
    final ObjectNode node = mapper.createObjectNode();
    node.put("test", "0");
    final OptionalLong result = JsonUtil.getOptionalLong(node, "test");
    assertThat(result).hasValue(0L);
  }

  @Test
  public void getValue_nonExistentKey() {
    final ObjectNode node = mapper.createObjectNode();
    final Optional<Integer> result = JsonUtil.getValue(node, "test", JsonNode::asInt);
    assertThat(result).isEmpty();
  }

  @Test
  public void getValue_nullValue() {
    final ObjectNode node = mapper.createObjectNode();
    node.set("test", null);
    final Optional<Integer> result = JsonUtil.getValue(node, "test", JsonNode::asInt);
    assertThat(result).isEmpty();
  }

  @Test
  public void getValue_validValue() {
    final ObjectNode node = mapper.createObjectNode();
    node.put("test", "123");
    final Optional<Integer> result = JsonUtil.getValue(node, "test", JsonNode::asInt);
    assertThat(result).hasValue(123);
  }

  @Test
  public void getValueWithDefault_nonExistentKey() {
    final String defaultVal = "defaultValue";
    final ObjectNode node = mapper.createObjectNode();
    final String result = JsonUtil.getValue(node, "test", JsonNode::asText, defaultVal);
    assertThat(result).isEqualTo(defaultVal);
  }

  @Test
  public void getValueWithDefault_nullValue() {
    final String defaultVal = "defaultValue";
    final ObjectNode node = mapper.createObjectNode();
    node.set("test", null);
    final String result = JsonUtil.getValue(node, "test", JsonNode::asText, defaultVal);
    assertThat(result).isEqualTo(defaultVal);
  }

  @Test
  public void getValueWithDefault_validValue() {
    final ObjectNode node = mapper.createObjectNode();
    node.put("test", "123");
    final String result = JsonUtil.getValue(node, "test", JsonNode::asText, "defaultVal");
    assertThat(result).isEqualTo("123");
  }

  @Test
  public void objectNodeFromMap() {
    final Map<String, Object> map = new TreeMap<>();
    map.put("a", 1);
    map.put("b", 2);

    final Map<String, Object> subMap = new TreeMap<>();
    subMap.put("c", "bla");
    subMap.put("d", 2L);
    map.put("subtree", subMap);

    ObjectNode node = JsonUtil.objectNodeFromMap(map);
    assertThat(node.get("a").asInt()).isEqualTo(1);
    assertThat(node.get("b").asInt()).isEqualTo(2);
    assertThat(node.get("subtree").get("c").asText()).isEqualTo("bla");
    assertThat(node.get("subtree").get("d").asLong()).isEqualTo(2L);
  }

  @Test
  public void objectNodeFromString() {
    final String jsonStr = "{\"a\":1, \"b\":2}";

    final ObjectNode result = JsonUtil.objectNodeFromString(jsonStr);
    assertThat(result.get("a").asInt()).isEqualTo(1);
    assertThat(result.get("b").asInt()).isEqualTo(2);
  }

  @Test
  public void getJson() throws JsonProcessingException {
    final String jsonStr = "{\"a\":1, \"b\":2}";
    final ObjectNode objectNode = JsonUtil.objectNodeFromString(jsonStr);

    final String resultUgly = JsonUtil.getJson(objectNode, false);
    final String resultPretty = JsonUtil.getJson(objectNode, true);

    assertThat(resultUgly).isEqualToIgnoringWhitespace(jsonStr);
    assertThat(resultPretty).isEqualToIgnoringWhitespace(jsonStr);
    // Pretty printed value should have more whitespace and contain returns
    assertThat(resultPretty.length()).isGreaterThan(resultUgly.length());
    assertThat(resultPretty).contains("\n");
    assertThat(resultUgly).doesNotContain("\n");
  }

  @Test
  public void getObjectNode_validValue() {
    final String jsonStr = "{\"test\": {\"a\":1, \"b\":2} }";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    final Optional<ObjectNode> maybeTestNode = JsonUtil.getObjectNode(rootNode, "test");
    assertThat(maybeTestNode).isNotEmpty();
    final ObjectNode testNode = maybeTestNode.get();
    assertThat(testNode.get("a").asInt()).isEqualTo(1);
    assertThat(testNode.get("b").asInt()).isEqualTo(2);
  }

  @Test
  public void getObjectNode_nullValue() {
    final String jsonStr = "{\"test\": null }";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    final Optional<ObjectNode> maybeTestNode = JsonUtil.getObjectNode(rootNode, "test");
    assertThat(maybeTestNode).isEmpty();
  }

  @Test
  public void getObjectNode_nonExistentKey() {
    final String jsonStr = "{}";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    final Optional<ObjectNode> maybeTestNode = JsonUtil.getObjectNode(rootNode, "test");
    assertThat(maybeTestNode).isEmpty();
  }

  @Test
  public void getObjectNode_wrongNodeType() {
    final String jsonStr = "{\"test\": \"abc\" }";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    assertThatThrownBy(() -> JsonUtil.getObjectNode(rootNode, "test"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected OBJECT node at \"test\" but got STRING");
  }

  @Test
  public void getArrayNode_validValue() {
    final String jsonStr = "{\"test\": [\"a\", \"b\"] }";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    final Optional<ArrayNode> maybeTestNode = JsonUtil.getArrayNode(rootNode, "test");
    assertThat(maybeTestNode).isNotEmpty();
    final ArrayNode testNode = maybeTestNode.get();
    assertThat(testNode.get(0).asText()).isEqualTo("a");
    assertThat(testNode.get(1).asText()).isEqualTo("b");
  }

  @Test
  public void getArrayNode_nullValue() {
    final String jsonStr = "{\"test\": null }";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    final Optional<ArrayNode> maybeTestNode = JsonUtil.getArrayNode(rootNode, "test");
    assertThat(maybeTestNode).isEmpty();
  }

  @Test
  public void getArrayNode_nonExistentKey() {
    final String jsonStr = "{}";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    final Optional<ArrayNode> maybeTestNode = JsonUtil.getArrayNode(rootNode, "test");
    assertThat(maybeTestNode).isEmpty();
  }

  @Test
  public void getArrayNode_wrongNodeType() {
    final String jsonStr = "{\"test\": \"abc\" }";
    final ObjectNode rootNode = JsonUtil.objectNodeFromString(jsonStr);

    assertThatThrownBy(() -> JsonUtil.getArrayNode(rootNode, "test"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected ARRAY node at \"test\" but got STRING");
  }
}
