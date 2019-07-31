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

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtil {
  public static OptionalLong getOptionalLong(final ObjectNode json, final String key) {
    return getValue(json, key, JsonNode::asLong).map(OptionalLong::of).orElse(OptionalLong.empty());
  }

  public static <T> Optional<T> getValue(
      final ObjectNode node, final String key, final Function<JsonNode, T> formatter) {
    JsonNode jsonValue = node.get(key);
    if (jsonValue == null || jsonValue.isNull()) {
      return Optional.empty();
    }
    return Optional.ofNullable(formatter.apply(jsonValue));
  }

  public static <T> T getValue(
      final ObjectNode node,
      final String key,
      final Function<JsonNode, T> formatter,
      final T defaultValue) {
    return getValue(node, key, formatter).orElse(defaultValue);
  }

  public static ObjectNode createEmptyObjectNode() {
    ObjectMapper mapper = getObjectMapper();
    return mapper.createObjectNode();
  }

  public static ObjectNode objectNodeFromMap(final Map<String, Object> map) {
    return (ObjectNode) getObjectMapper().valueToTree(map);
  }

  public static ObjectNode objectNodeFromString(final String jsonData) {
    return objectNodeFromString(jsonData, false);
  }

  public static ObjectNode objectNodeFromString(
      final String jsonData, final boolean allowComments) {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(Feature.ALLOW_COMMENTS, allowComments);
    try {
      final JsonNode jsonNode = objectMapper.readTree(jsonData);
      if (!jsonNode.isObject()) {
        throw new IllegalArgumentException(
            "JSON string must represent a json object, but "
                + jsonNode.getNodeType()
                + " supplied.");
      }
      return (ObjectNode) jsonNode;
    } catch (IOException e) {
      // Reading directly from a string should not raise an IOException, just catch and rethrow
      throw new RuntimeException(e);
    }
  }

  public static String getJson(final Object objectNode) throws JsonProcessingException {
    return getJson(objectNode, true);
  }

  public static String getJson(final Object objectNode, final boolean prettyPrint)
      throws JsonProcessingException {
    ObjectMapper mapper = getObjectMapper();
    if (prettyPrint) {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
    } else {
      return mapper.writeValueAsString(objectNode);
    }
  }

  public static ObjectMapper getObjectMapper() {
    return new ObjectMapper();
  }

  public static Optional<ObjectNode> getObjectNode(final ObjectNode json, final String fieldKey) {
    return getObjectNode(json, fieldKey, true);
  }

  public static Optional<ObjectNode> getObjectNode(
      final ObjectNode json, final String fieldKey, final boolean strict) {
    final JsonNode obj = json.get(fieldKey);
    if (obj == null || obj.isNull()) {
      return Optional.empty();
    }

    if (!obj.isObject()) {
      if (strict) {
        throw new IllegalArgumentException(
            String.format(
                "Expected OBJECT node at \"%s\" but got %s",
                fieldKey, obj.getNodeType().toString()));
      } else {
        return Optional.empty();
      }
    }

    return Optional.of((ObjectNode) obj);
  }

  public static Optional<ArrayNode> getArrayNode(final ObjectNode json, final String fieldKey) {
    return getArrayNode(json, fieldKey, true);
  }

  public static Optional<ArrayNode> getArrayNode(
      final ObjectNode json, final String fieldKey, final boolean strict) {
    final JsonNode obj = json.get(fieldKey);
    if (obj == null || obj.isNull()) {
      return Optional.empty();
    }

    if (!obj.isArray()) {
      if (strict) {
        throw new IllegalArgumentException(
            String.format(
                "Expected ARRAY node at \"%s\" but got %s",
                fieldKey, obj.getNodeType().toString()));
      } else {
        return Optional.empty();
      }
    }

    return Optional.of((ArrayNode) obj);
  }
}
