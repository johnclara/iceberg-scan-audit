package com.box.dataplatform.iceberg.encryption.crypto;

import com.box.dataplatform.crypto.BCEncryptor;
import com.box.dataplatform.crypto.Encryptor;
import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.LoadableBuilder;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SerializableSupplier;

public class AesGcmFormat implements SymmetricKeyCryptoFormat {
  private static final String NAME = "aes.gcm";
  public static final int DEK_LENGTH = 32;
  public static final int AUTH_TAG_LENGTH = Encryptor.GCM_AUTH_TAG_BYTE_LENGTH;
  private final SerializableSupplier<Encryptor> encryptorSupplier;
  private transient Encryptor encryptor;

  private Encryptor getEncryptor() {
    if (encryptor == null) {
      encryptor = encryptorSupplier.get();
    }
    return encryptor;
  }

  /** * CONSTRUCTOR ** */
  public AesGcmFormat(SerializableSupplier<Encryptor> encryptorSupplier) {
    this.encryptorSupplier = encryptorSupplier;
  }

  /** * LOGIC ** */
  @Override
  public PositionOutputStream encryptionStream(
      PositionOutputStream plaintextStream, byte[] plaintextDek, byte[] iv) {
    return new EncryptingPositionOutputStream(plaintextStream, plaintextDek, getEncryptor());
  }

  @Override
  public long plaintextLength(long encryptedLength) {
    return encryptedLength - AUTH_TAG_LENGTH;
  }

  @Override
  public int dekLength() {
    return DEK_LENGTH;
  }

  @Override
  public SeekableInputStream decryptionStream(
      SeekableInputStream encryptedStream, Long rawLength, byte[] plaintextDek, byte[] iv) {
    return new DecryptingSeekableInputStream(
        encryptedStream, rawLength, plaintextDek, getEncryptor());
  }

  /** * BUILDER ** */
  public static AesGcmFormat load(Conf conf) {
    return builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, AesGcmFormat> {
    SerializableSupplier<Encryptor> encryptorSupplier = () -> BCEncryptor.INSTANCE;

    public Builder() {}

    @Override
    public Builder load(Conf conf) {
      return this;
    }

    @Override
    public AesGcmFormat build() {
      return new AesGcmFormat(encryptorSupplier);
    }
  }
}
