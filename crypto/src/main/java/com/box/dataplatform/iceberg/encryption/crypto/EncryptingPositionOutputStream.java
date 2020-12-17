package com.box.dataplatform.iceberg.encryption.crypto;

import com.box.dataplatform.crypto.Encryptor;
import java.io.IOException;
import javax.crypto.CipherOutputStream;
import org.apache.iceberg.io.PositionOutputStream;

public class EncryptingPositionOutputStream extends PositionOutputStream {
  private final PositionOutputStream rawStream;
  private final byte[] plaintextDek;
  private final CipherOutputStream encryptedStream;

  private Long pos = 0L;

  public EncryptingPositionOutputStream(
      PositionOutputStream rawStream, byte[] plaintextDek, Encryptor encryptor) {
    this.rawStream = rawStream;
    this.plaintextDek = plaintextDek;
    this.encryptedStream =
        new CipherOutputStream(rawStream, encryptor.gcmEncryptionBuilder(plaintextDek).build());
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void write(int b) throws IOException {
    encryptedStream.write(b);
    pos += 1;
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    encryptedStream.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
    encryptedStream.close();
  }
}
