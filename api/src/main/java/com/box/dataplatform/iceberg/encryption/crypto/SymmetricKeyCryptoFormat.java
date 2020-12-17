package com.box.dataplatform.iceberg.encryption.crypto;

import java.io.Serializable;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

public interface SymmetricKeyCryptoFormat extends Serializable {
  SeekableInputStream decryptionStream(
      SeekableInputStream encryptedStream, Long rawLength, byte[] plaintextDek, byte[] iv);

  PositionOutputStream encryptionStream(
      PositionOutputStream plaintextStream, byte[] plaintextDek, byte[] iv);

  long plaintextLength(long encryptedLength);

  int dekLength();

  default int ivLength() {
    return 0;
  }
}
