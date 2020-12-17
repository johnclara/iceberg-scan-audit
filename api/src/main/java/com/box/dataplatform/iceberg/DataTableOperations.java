package com.box.dataplatform.iceberg;

import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.iceberg.encryption.SymmetricKeyEncryptionManager;
import com.box.dataplatform.iceberg.encryption.crypto.SymmetricKeyCryptoFormat;
import com.box.dataplatform.iceberg.encryption.dekprovider.DekProvider;
import com.box.dataplatform.iceberg.io.DPFileIO;
import com.box.dataplatform.iceberg.metastore.Metastore;
import com.box.dataplatform.iceberg.util.MapSerde;
import org.apache.iceberg.encryption.EncryptionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTableOperations<K extends KekId> extends AllTypesTableOperations {
  public static Logger log = LoggerFactory.getLogger(DataTableOperations.class);

  private final SymmetricKeyCryptoFormat cryptoFormat;
  private final MapSerde serde;

  private final DekProvider<K> dekProvider;
  private final K encryptedKekId;
  private final K plaintextKekid;

  public DataTableOperations(
      String tenantName,
      String icebergTableName,
      Metastore metastore,
      DPFileIO dpFileIO,
      SymmetricKeyCryptoFormat cryptoFormat,
      MapSerde serde,
      DekProvider<K> dekProvider,
      K encryptedKekId,
      K plaintextKekid) {
    super(tenantName, icebergTableName, metastore, dpFileIO);

    this.cryptoFormat = cryptoFormat;
    this.serde = serde;
    this.dekProvider = dekProvider;
    this.encryptedKekId = encryptedKekId;
    this.plaintextKekid = plaintextKekid;
  }

  private boolean shouldEncryptManifest() {
    return current()
        .propertyAsBoolean(
            DPTableProperties.MANIFEST_ENCRYPTED, DPTableProperties.MANIFEST_ENCRYPTED_DEFAULT);
  }

  public String encryptionType() {
    return current()
        .property(DPTableProperties.ENCRYPTION_TYPE, DPTableProperties.ENCRYPTION_TYPE_DEFAULT);
  }

  @Override
  public EncryptionManager encryption() {
    String encryptionType = encryptionType();

    K kekId;
    if (!shouldEncryptManifest()) {
      kekId = encryptedKekId;
    } else {
      kekId = plaintextKekid;
    }

    return new SymmetricKeyEncryptionManager<K>(dekProvider, kekId, cryptoFormat, serde);
  }
}
