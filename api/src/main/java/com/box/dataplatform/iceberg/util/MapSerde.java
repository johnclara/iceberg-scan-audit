package com.box.dataplatform.iceberg.util;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

public interface MapSerde extends Serializable {
  Map<String, String> fromBytes(ByteBuffer bytes) throws IOException;

  byte[] toBytes(Map<String, String> map) throws IOException;
}
