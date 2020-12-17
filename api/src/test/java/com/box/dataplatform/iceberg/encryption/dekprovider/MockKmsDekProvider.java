package com.box.dataplatform.iceberg.encryption.dekprovider;

public class MockKmsDekProvider extends AbstractKmsDekProvider {
  private final MockKms kms;

  public MockKmsDekProvider(MockKms kms) {
    this.kms = kms;
  }

  @Override
  protected KmsGenerateDekResponse getDekFromKms(KmsKekId kmsKekId, int numBytes) {
    return kms.getPlaintextFromKms(kmsKekId, numBytes);
  }

  @Override
  protected KmsDecryptResponse getPlaintextFromKms(KmsKekId kmsKekId, byte[] encryptedDek) {
    return kms.getPlaintextFromKms(kmsKekId, encryptedDek);
  }
}
