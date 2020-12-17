package com.box.dataplatform.iceberg.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonMapSerde implements MapSerde {
  private static final Logger log = LoggerFactory.getLogger(JsonMapSerde.class);

  private JsonMapSerde() {
  }

  public static final JsonMapSerde INSTANCE = new JsonMapSerde();

  @Override
  public Map<String, String> fromBytes(ByteBuffer bytes) throws IOException {
    JsonNode jsonNode = JsonUtil.mapper().readValue(bytes.array(), JsonNode.class);
    if (jsonNode.getNodeType() != JsonNodeType.OBJECT) {
      throw new IllegalArgumentException(
          String.format(
              "Can't deserialize Json as a Json Object! Got %s instead.", jsonNode.getNodeType()));
    }
    Map<String, String> map = new HashMap<>();
    for (Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> entry = it.next();
      String property = entry.getKey();
      JsonNode value = entry.getValue();
      if (value.getNodeType() != JsonNodeType.STRING) {
        throw new IllegalArgumentException(
            String.format(
                "Can't deserialize json field %s as a string! Got %s instead.",
                property, value.getNodeType()));
      }

      map.put(property, value.textValue());
    }
    return map;
  }

  @Override
  public byte[] toBytes(Map<String, String> map) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (JsonGenerator generator = JsonUtil.factory().createGenerator(byteArrayOutputStream)) {
      generator.writeStartObject();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        generator.writeStringField(entry.getKey(), entry.getValue());
      }
      generator.writeEndObject();
      generator.flush();
    }
    byteArrayOutputStream.flush();
    byteArrayOutputStream.close();
    return byteArrayOutputStream.toByteArray();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    return this.getClass() == o.getClass();
  }
}
