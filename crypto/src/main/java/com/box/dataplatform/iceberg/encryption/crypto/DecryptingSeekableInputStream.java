package com.box.dataplatform.iceberg.encryption.crypto;

import com.box.dataplatform.crypto.Encryptor;
import java.io.IOException;
import java.io.InputStream;
import javax.crypto.CipherInputStream;
import org.apache.iceberg.io.SeekableInputStream;

public class DecryptingSeekableInputStream extends SeekableInputStream {
  private final Encryptor encryptor;

  private final SeekableInputStream rawStream;
  private final byte[] plaintextDek;
  private final Long rawLength;

  private CipherInputStream decryptingStream;
  private long pos;

  public DecryptingSeekableInputStream(
      SeekableInputStream rawStream, Long rawLength, byte[] plaintextDek, Encryptor encryptor) {
    this.rawStream = rawStream;
    this.plaintextDek = plaintextDek;
    this.rawLength = rawLength;
    this.decryptingStream =
        new CipherInputStream(rawStream, encryptor.gcmDecryptionBuilder(plaintextDek).build());
    this.encryptor = encryptor;
    pos = 0L;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long newPos) throws IOException {
    rawStream.seek(newPos);
    InputStream truncatedStream =
        new TruncatedInputStream(rawStream, rawLength - AesGcmFormat.AUTH_TAG_LENGTH - newPos);
    try {
      decryptingStream =
          new CipherInputStream(
              truncatedStream,
              encryptor.gcmDecryptionBuilder(plaintextDek).withOffset(newPos).build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    pos = newPos;
  }

  @Override
  public int read() throws IOException {
    int result = decryptingStream.read();
    if (result >= 0) {
      pos += 1;
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    super.close();
    decryptingStream.close();
  }
}
