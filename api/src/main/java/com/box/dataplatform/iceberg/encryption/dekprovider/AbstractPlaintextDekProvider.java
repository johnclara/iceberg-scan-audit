package com.box.dataplatform.iceberg.encryption.dekprovider;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.util.Conf;
import java.util.Arrays;

public abstract class AbstractPlaintextDekProvider
    extends DekProvider<AbstractPlaintextDekProvider.PlaintextKekId> {
  public static final String NAME = "plaintext";

  // visible for testing
  protected static final byte[] DUMMY_IV = new byte[0];

  protected abstract byte[] generateRandomBytes(int numBytes);

  @Override
  public Dek getNewDek(PlaintextKekId kekId, int dekLength, int ivLength) {
    byte[] encryptedDek = generateRandomBytes(dekLength);
    return new Dek(encryptedDek, Arrays.copyOf(encryptedDek, encryptedDek.length), DUMMY_IV);
  }

  @Override
  public Dek getPlaintextDek(PlaintextKekId kekId, Dek dek) {
    dek.setPlaintextDek(Arrays.copyOf(dek.encryptedDek(), dek.encryptedDek().length));
    return dek;
  }

  @Override
  public PlaintextKekId loadKekId(Conf conf) {
    return PlaintextKekId.INSTANCE;
  }

  public static class PlaintextKekId implements KekId {
    private PlaintextKekId() {}

    public static final PlaintextKekId INSTANCE = new PlaintextKekId();

    @Override
    public String toString() {
      return "PlaintextKekId{}";
    }

    @Override
    public void dump(Conf conf) {}
  }
}
