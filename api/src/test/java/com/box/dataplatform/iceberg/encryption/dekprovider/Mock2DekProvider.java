package com.box.dataplatform.iceberg.encryption.dekprovider;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.LoadableBuilder;
import java.util.Objects;

public class Mock2DekProvider extends DekProvider<Mock2DekProvider.Mock2KekId> {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String MY_FIELD = "myField2";

  public static final String MY_OTHER_FIELD = "myOtherField2";

  public static final byte[] BYTES = new byte[1];

  /** * CONSTRUCTOR ** */
  public Mock2DekProvider() {}

  /** * LOGIC ** */
  @Override
  public Dek getNewDek(Mock2KekId kekId, int dekLength, int ivLength) {
    return Dek.builder().setEncryptedDek(BYTES).setIv(BYTES).build();
  }

  @Override
  public Dek getPlaintextDek(Mock2KekId kekId, Dek dek) {
    dek.setPlaintextDek(BYTES);
    return dek;
  }

  @Override
  public String toString() {
    return "Mock2DekProvider{}";
  }

  @Override
  public Mock2KekId loadKekId(Conf conf) {
    int myValue = conf.propertyAsInt(MY_FIELD);
    double myOtherValue = conf.propertyAsDouble(MY_OTHER_FIELD);
    return new Mock2KekId(myValue, myOtherValue);
  }

  public static class Mock2KekId implements KekId {
    private final int myValue;
    private final double myOtherValue;

    public Mock2KekId(int myValue, double myOtherValue) {
      this.myValue = myValue;
      this.myOtherValue = myOtherValue;
    }

    @Override
    public void dump(Conf conf) {
      conf.setInt(MY_FIELD, myValue);
      conf.setDouble(MY_OTHER_FIELD, myOtherValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Mock2KekId)) return false;
      Mock2KekId mock2KekId = (Mock2KekId) o;
      return myOtherValue == mock2KekId.myOtherValue && Objects.equals(myValue, mock2KekId.myValue);
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

  public static class Builder implements LoadableBuilder<Builder, Mock2DekProvider> {

    @Override
    public Builder load(Conf conf) {
      return this;
    }

    @Override
    public Mock2DekProvider build() {
      return new Mock2DekProvider();
    }
  }
}
