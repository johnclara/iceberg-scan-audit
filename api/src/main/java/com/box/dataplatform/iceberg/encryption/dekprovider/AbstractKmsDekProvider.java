package com.box.dataplatform.iceberg.encryption.dekprovider;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.util.Conf;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.util.Tasks;

public abstract class AbstractKmsDekProvider extends DekProvider<AbstractKmsDekProvider.KmsKekId> {
  public static final String NAME = "kms";
  public static final String KEK_ID = "kekId";

  // Visible for testing
  protected static final byte[] DUMMY_IV = new byte[0];

  protected abstract KmsGenerateDekResponse getDekFromKms(KmsKekId kmsKekId, int numBytes);

  protected abstract KmsDecryptResponse getPlaintextFromKms(KmsKekId kmsKekId, byte[] encryptedDek);

  @Override
  public KmsKekId loadKekId(Conf conf) {
    String kekId = conf.propertyAsString(KEK_ID);
    return KmsKekId.of(kekId);
  }

  @Override
  public Dek getNewDek(KmsKekId kekId, int dekLength, int ivLength) {
    AtomicReference<KmsGenerateDekResponse> kmsResponseHolder = new AtomicReference<>();

    // Based on: BaseMetastoreTableOperations#refreshFromMetadataLocation
    Tasks.foreach(0)
        .retry(5000)
        .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
        .throwFailureWhenFinished()
        .run(i -> kmsResponseHolder.set(getDekFromKms(kekId, dekLength)));

    KmsGenerateDekResponse kmsResponse = kmsResponseHolder.get();
    return new Dek(kmsResponse.encryptedDek, kmsResponse.plaintextDek, DUMMY_IV);
  }

  @Override
  public Dek getPlaintextDek(KmsKekId kekId, Dek dek) {
    AtomicReference<KmsDecryptResponse> kmsResponseHolder = new AtomicReference<>();

    // Based on: BaseMetastoreTableOperations#refreshFromMetadataLocation
    Tasks.foreach(0)
        .retry(5000)
        .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
        .throwFailureWhenFinished()
        .run(i -> kmsResponseHolder.set(getPlaintextFromKms(kekId, dek.encryptedDek())));

    KmsDecryptResponse kmsResponse = kmsResponseHolder.get();
    dek.setPlaintextDek(kmsResponse.plaintextDek);
    return dek;
  }

  protected static class KmsGenerateDekResponse {
    private final byte[] encryptedDek;
    private final byte[] plaintextDek;

    public KmsGenerateDekResponse(byte[] encryptedDek, byte[] plaintextDek) {
      this.encryptedDek = encryptedDek;
      this.plaintextDek = plaintextDek;
    }

    public byte[] encryptedDek() {
      return encryptedDek;
    }

    public byte[] plaintextDek() {
      return plaintextDek;
    }
  }

  protected static class KmsDecryptResponse {
    private final byte[] plaintextDek;

    public KmsDecryptResponse(byte[] plaintextDek) {
      this.plaintextDek = plaintextDek;
    }

    public byte[] plaintextDek() {
      return plaintextDek;
    }
  }

  public static class KmsKekId implements KekId {

    private final String kmsKeyId;

    protected KmsKekId(String kmsKeyId) {
      this.kmsKeyId = kmsKeyId;
    }

    public static KmsKekId of(String kmsKeyId) {
      return new KmsKekId(kmsKeyId);
    }

    @Override
    public void dump(Conf conf) {
      conf.setString(KEK_ID, kmsKeyId);
    }

    public String kmsKeyId() {
      return kmsKeyId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (this.getClass() != o.getClass()) return false;
      KmsKekId kmsKekId = (KmsKekId) o;
      return Objects.equals(this.kmsKeyId, kmsKekId.kmsKeyId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kmsKeyId);
    }

    @Override
    public String toString() {
      return "KmsKekId{" + "kmsKeyId='" + kmsKeyId + '\'' + '}';
    }
  }
}
