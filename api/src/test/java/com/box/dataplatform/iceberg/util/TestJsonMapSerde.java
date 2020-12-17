package com.box.dataplatform.iceberg.util;

import static com.box.dataplatform.test.util.SerializableUtil.deserialize;
import static com.box.dataplatform.test.util.SerializableUtil.serialize;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestJsonMapSerde {
  String jsonString;
  Map<String, String> map;
  MapSerde serde;

  @Before
  public void setup() {
    map = new HashMap<>();
    map.put("hello", "World");
    map.put("foo", "baR");
    jsonString = "{\"foo\":\"baR\", \"hello\":     \"World\"  }"; // supposed to be wonky
    serde = JsonMapSerde.INSTANCE;
  }

  @Test
  public void serializedDeserialize() throws IOException {
    byte[] bytes = serde.toBytes(map);
    Map<String, String> actual = serde.fromBytes(ByteBuffer.wrap(bytes));
    Assert.assertEquals(map, actual);
  }

  @Test
  public void fromString() throws IOException {
    byte[] bytes = jsonString.getBytes(StandardCharsets.UTF_8);
    Map<String, String> actual = serde.fromBytes(ByteBuffer.wrap(bytes));
    Assert.assertEquals(map, actual);
  }

  @Test
  public void testSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    byte[] serialized1 = serialize(serde);
    byte[] serialized2 = serialize(serde);

    Object deserialized1 = deserialize(serialized1);
    Object deserialized2 = deserialize(serialized2);
    Assert.assertEquals(deserialized1, deserialized2);
    Assert.assertEquals(serde, deserialized1);
    Assert.assertEquals(serde, deserialized2);
  }
}
