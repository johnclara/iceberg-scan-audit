package com.box.dataplatform.iceberg.encryption;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;

public class BasicEncryptionKeyMetadata implements EncryptionKeyMetadata {
  private final byte[] metadata;

  public BasicEncryptionKeyMetadata(byte[] metadata) {
    this.metadata = metadata;
  }

  public ByteBuffer buffer() {
    return ByteBuffer.wrap(metadata);
  }

  public EncryptionKeyMetadata copy() {
    return new BasicEncryptionKeyMetadata(Arrays.copyOf(metadata, metadata.length));
  }
}
