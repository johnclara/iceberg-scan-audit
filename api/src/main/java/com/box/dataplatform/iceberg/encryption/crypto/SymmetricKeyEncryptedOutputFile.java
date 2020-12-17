package com.box.dataplatform.iceberg.encryption.crypto;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

public class SymmetricKeyEncryptedOutputFile implements OutputFile {
  private final OutputFile plaintextOutputFile;
  private final SymmetricKeyCryptoFormat symmetricKeyCryptoFormat;
  private final byte[] plaintextDek;
  private final byte[] iv;

  public SymmetricKeyEncryptedOutputFile(
      OutputFile plaintextOutputFile,
      SymmetricKeyCryptoFormat symmetricKeyCryptoFormat,
      byte[] plaintextDek,
      byte[] iv) {
    this.plaintextOutputFile = plaintextOutputFile;
    this.plaintextDek = plaintextDek;
    this.symmetricKeyCryptoFormat = symmetricKeyCryptoFormat;
    this.iv = iv;
  }

  @Override
  public PositionOutputStream create() {
    PositionOutputStream plaintextOutputStream = plaintextOutputFile.create();
    return symmetricKeyCryptoFormat.encryptionStream(plaintextOutputStream, plaintextDek, iv);
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    PositionOutputStream plaintextOutputStream = plaintextOutputFile.createOrOverwrite();
    return symmetricKeyCryptoFormat.encryptionStream(plaintextOutputStream, plaintextDek, iv);
  }

  @Override
  public String location() {
    return plaintextOutputFile.location();
  }

  @Override
  public InputFile toInputFile() {
    return plaintextOutputFile.toInputFile();
  }
}
