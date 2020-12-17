package com.box.dataplatform.iceberg.encryption.dekprovider;

import java.io.Serializable;

public interface MockKms extends Serializable {
  AbstractKmsDekProvider.KmsGenerateDekResponse getPlaintextFromKms(
      AbstractKmsDekProvider.KmsKekId kmsKekId, int numBytes);

  AbstractKmsDekProvider.KmsDecryptResponse getPlaintextFromKms(
      AbstractKmsDekProvider.KmsKekId kmsKekId, byte[] encryptedDek);
}
