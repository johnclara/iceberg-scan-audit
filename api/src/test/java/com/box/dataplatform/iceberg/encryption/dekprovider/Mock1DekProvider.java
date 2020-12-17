package com.box.dataplatform.iceberg.encryption.dekprovider;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.LoadableBuilder;
import java.util.Objects;

public class Mock1DekProvider extends DekProvider<Mock1DekProvider.Mock1KekId> {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String MY_FIELD = "myField1";

  public static final String MY_OTHER_FIELD = "myOtherField1";
  public static final byte[] BYTES = new byte[0];

  /** * CONSTRUCTOR ** */
  public Mock1DekProvider() {}

  /** * LOGIC ** */
  @Override
  public Dek getNewDek(Mock1KekId kekId, int dekLength, int ivLength) {
    return Dek.builder().setEncryptedDek(BYTES).setIv(BYTES).build();
  }

  @Override
  public Dek getPlaintextDek(Mock1KekId kekId, Dek dek) {
    dek.setPlaintextDek(BYTES);
    return dek;
  }

  @Override
  public String toString() {
    return "Mock1DekProvider{}";
  }

  @Override
  public Mock1KekId loadKekId(Conf conf) {
    String myValue = conf.propertyAsString(MY_FIELD);
    int myOtherValue = conf.propertyAsInt(MY_OTHER_FIELD);
    return new Mock1KekId(myValue, myOtherValue);
  }

  public static class Mock1KekId implements KekId {
    private final String myValue;
    private final int myOtherValue;

    public Mock1KekId(String myValue, int myOtherValue) {
      this.myValue = myValue;
      this.myOtherValue = myOtherValue;
    }

    @Override
    public void dump(Conf conf) {
      conf.setString(MY_FIELD, myValue);
      conf.setInt(MY_OTHER_FIELD, myOtherValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Mock1KekId)) return false;
      Mock1KekId mockKekId = (Mock1KekId) o;
      return myOtherValue == mockKekId.myOtherValue && Objects.equals(myValue, mockKekId.myValue);
    }

    @Override
    public int hashCode() {
      return Objects.hash(myValue, myOtherValue);
    }

    @Override
    public String toString() {
      return "MockKekId{" + "myValue='" + myValue + '\'' + ", myOtherValue=" + myOtherValue + '}';
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    return this.getClass() == o.getClass();
  }

  public static class Builder implements LoadableBuilder<Builder, Mock1DekProvider> {

    @Override
    public Builder load(Conf conf) {
      return this;
    }

    @Override
    public Mock1DekProvider build() {
      return new Mock1DekProvider();
    }
  }
}
