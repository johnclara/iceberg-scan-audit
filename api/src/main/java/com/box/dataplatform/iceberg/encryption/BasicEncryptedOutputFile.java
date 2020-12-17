package com.box.dataplatform.iceberg.encryption;

import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.OutputFile;

public class BasicEncryptedOutputFile implements EncryptedOutputFile {
  private final OutputFile outputFile;
  private final EncryptionKeyMetadata encryptionKeyMetadata;

  public BasicEncryptedOutputFile(
      OutputFile outputFile, EncryptionKeyMetadata encryptionKeyMetadata) {
    this.outputFile = outputFile;
    this.encryptionKeyMetadata = encryptionKeyMetadata;
  }

  @Override
  public OutputFile encryptingOutputFile() {
    return outputFile;
  }

  @Override
  public EncryptionKeyMetadata keyMetadata() {
    return encryptionKeyMetadata;
  }
}
