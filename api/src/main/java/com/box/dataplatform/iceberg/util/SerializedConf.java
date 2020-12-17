package com.box.dataplatform.iceberg.util;

import com.box.dataplatform.util.Conf;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper for a {@code Map<String, String>} that gets serialized and deserialized into a byte
 * buffer.
 */
public class SerializedConf extends Conf {
  private static final Logger log = LoggerFactory.getLogger(SerializedConf.class);
  private final Map<String, String> innerMap;
  private final MapSerde serde;

  protected SerializedConf(MapSerde serde) {
    this(serde, new HashMap<>());
  }

  protected SerializedConf(MapSerde serde, ByteBuffer bytes) {
    this(serde, fromBytes(serde, bytes));
  }

  private SerializedConf(MapSerde serde, Map<String, String> innerMap) {
    super(""); // no namespace
    this.serde = serde;
    this.innerMap = innerMap;
  }

  @Override
  protected String get(String key) {
    return innerMap.get(key);
  }

  @Override
  protected void set(String key, String value) {
    innerMap.put(key, value);
  }

  private static Map<String, String> fromBytes(MapSerde serde, ByteBuffer bytes) {
    try {
      return serde.fromBytes(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public byte[] toBytes() {
    try {
      return serde.toBytes(innerMap);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static SerializedConf of(MapSerde serde, ByteBuffer bytes) {
    return new SerializedConf(serde, bytes);
  }

  public static SerializedConf empty(MapSerde serde) {
    return new SerializedConf(serde);
  }
}
