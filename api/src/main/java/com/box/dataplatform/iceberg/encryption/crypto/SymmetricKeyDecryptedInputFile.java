package com.box.dataplatform.iceberg.encryption.crypto;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

public class SymmetricKeyDecryptedInputFile implements InputFile {
  private final InputFile encryptedInputFile;
  private final SymmetricKeyCryptoFormat symmetricKeyCryptoFormat;
  private final byte[] plaintextDek;
  private final byte[] iv;

  public SymmetricKeyDecryptedInputFile(
      InputFile encryptedInputFile,
      SymmetricKeyCryptoFormat symmetricKeyCryptoFormat,
      byte[] plaintextDek,
      byte[] iv) {
    this.encryptedInputFile = encryptedInputFile;
    this.symmetricKeyCryptoFormat = symmetricKeyCryptoFormat;
    this.plaintextDek = plaintextDek;
    this.iv = iv;
  }

  @Override
  public long getLength() {
    return symmetricKeyCryptoFormat.plaintextLength(encryptedInputFile.getLength());
  }

  @Override
  public SeekableInputStream newStream() {
    SeekableInputStream encryptedInputStream = encryptedInputFile.newStream();
    return symmetricKeyCryptoFormat.decryptionStream(
        encryptedInputStream, encryptedInputFile.getLength(), plaintextDek, iv);
  }

  @Override
  public String location() {
    return encryptedInputFile.location();
  }

  @Override
  public boolean exists() {
    return encryptedInputFile.exists();
  }
}
