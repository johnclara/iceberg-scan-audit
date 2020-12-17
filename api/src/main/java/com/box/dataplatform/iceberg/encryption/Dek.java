package com.box.dataplatform.iceberg.encryption;

import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.Dumpable;
import com.box.dataplatform.util.LoadableBuilder;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dek implements Serializable, Dumpable {
  private static final Logger log = LoggerFactory.getLogger(Dek.ENCRYPTED_DEK);
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String ENCRYPTED_DEK = "encodedDek";

  public static final String IV = "encodedIv";

  /** * PRIVATE VARIABLES ** */
  private final byte[] encryptedDek;

  private transient byte[] plaintextDek;
  private final byte[] iv;

  /** * CONSTRUCTOR ** */
  public Dek(byte[] encryptedDek, byte[] plaintextDek, byte[] iv) {
    this.encryptedDek = encryptedDek;
    this.plaintextDek = plaintextDek;
    this.iv = iv;
  }

  public Dek(byte[] encryptedDek, byte[] iv) {
    this.encryptedDek = encryptedDek;
    this.plaintextDek = null;
    this.iv = iv;
  }

  public static Dek load(Conf conf) {
    return new Dek.Builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, Dek> {
    private byte[] encryptedDek;
    private byte[] plaintextDek;
    private byte[] iv;

    public Builder setEncryptedDek(byte[] encryptedDek) {
      this.encryptedDek = encryptedDek;
      return this;
    }

    public Builder setPlaintextDek(byte[] plaintextDek) {
      this.plaintextDek = plaintextDek;
      return this;
    }

    public Builder setIv(byte[] iv) {
      this.iv = iv;
      return this;
    }

    public Builder load(Conf conf) {
      Base64.Decoder decoder = Base64.getDecoder();
      encryptedDek =
          decoder.decode(conf.propertyAsString(ENCRYPTED_DEK).getBytes(StandardCharsets.UTF_8));
      iv = decoder.decode(conf.propertyAsString(IV));
      return this;
    }

    @Override
    public Dek build() {
      return new Dek(encryptedDek, plaintextDek, iv);
    }
  }

  /** * DUMPER ** */
  public void dump(Conf conf) {
    Base64.Encoder encoder = Base64.getEncoder();
    conf.setString(ENCRYPTED_DEK, new String(encoder.encode(encryptedDek), StandardCharsets.UTF_8));
    conf.setString(IV, new String(encoder.encode(iv), StandardCharsets.UTF_8));
  }

  /** * GETTERS ** */
  public byte[] encryptedDek() {
    return encryptedDek;
  }

  public byte[] iv() {
    return iv;
  }

  public byte[] plaintextDek() {
    return plaintextDek;
  }

  public void setPlaintextDek(byte[] plaintextDek) {
    this.plaintextDek = plaintextDek;
  }

  /** EQUALS HASH CODE WARNING: plaintext dek is not checked in equals and hashcode */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (this.getClass() != o.getClass()) return false;
    Dek dek = (Dek) o;
    return Arrays.equals(encryptedDek, dek.encryptedDek) && Arrays.equals(iv, dek.iv);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(encryptedDek);
    result = 31 * result + Arrays.hashCode(iv);
    return result;
  }

  /** TO STRING WARNING: plaintext dek should be redacted */
  @Override
  public String toString() {
    return "Dek{"
        + "encryptedDek="
        + "REDACTED"
        + ", plaintextDek="
        + "REDACTED"
        + ", iv="
        + Arrays.toString(iv)
        + '}';
  }
}
