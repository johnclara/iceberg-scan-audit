package com.box.dataplatform.iceberg.encryption.dekprovider;

public class MockPlaintextDekProvider extends AbstractPlaintextDekProvider {
  public static final byte[] BYTES = new byte[4];

  @Override
  protected byte[] generateRandomBytes(int numBytes) {
    return BYTES;
  }
}
