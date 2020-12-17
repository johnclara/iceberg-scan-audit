package com.box.dataplatform.iceberg.encryption.crypto;

import java.io.IOException;
import java.io.InputStream;

public class TruncatedInputStream extends InputStream {
  public static final int DEK_LENGTH = 32;
  public static final int AUTH_TAG_LENGTH = 16;

  private final InputStream inputStream;
  private final Long maxLength;
  private long currentPosition = 0;

  public TruncatedInputStream(InputStream inputStream, Long maxLength) {
    this.inputStream = inputStream;
    this.maxLength = maxLength;
  }

  @Override
  public int read() throws IOException {
    if (currentPosition >= maxLength) {
      return -1;
    }

    int result = inputStream.read();
    if (result >= 0) {
      currentPosition += 1;
    }

    return result;
  }

  @Override
  public void close() throws IOException {
    super.close();
    inputStream.close();
  }
}
