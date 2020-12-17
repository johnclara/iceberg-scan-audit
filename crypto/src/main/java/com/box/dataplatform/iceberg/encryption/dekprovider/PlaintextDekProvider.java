package com.box.dataplatform.iceberg.encryption.dekprovider;

import com.box.dataplatform.crypto.BCEncryptor;

public class PlaintextDekProvider extends AbstractPlaintextDekProvider {
  public PlaintextDekProvider() {}

  @Override
  protected byte[] generateRandomBytes(int numBytes) {
    return BCEncryptor.INSTANCE.generateRandomBytes(numBytes);
  }
}
