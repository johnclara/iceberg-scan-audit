package com.box.dataplatform.iceberg.encryption;

import com.box.dataplatform.iceberg.encryption.crypto.SymmetricKeyCryptoFormat;
import com.box.dataplatform.iceberg.encryption.crypto.SymmetricKeyDecryptedInputFile;
import com.box.dataplatform.iceberg.encryption.crypto.SymmetricKeyEncryptedOutputFile;
import com.box.dataplatform.iceberg.encryption.dekprovider.DekProvider;
import com.box.dataplatform.iceberg.util.MapSerde;
import com.box.dataplatform.iceberg.util.SerializedConf;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SymmetricKeyEncryptionManager<K extends KekId> implements EncryptionManager {
  private static final Logger log = LoggerFactory.getLogger(SymmetricKeyEncryptionManager.class);
  private final DekProvider<K> dekProvider;
  private final K kekIdForWriting;
  private final SymmetricKeyCryptoFormat symmetricKeyCryptoFormat;
  private final MapSerde serde;

  public SymmetricKeyEncryptionManager(
      DekProvider<K> dekProvider,
      K kekIdForWriting,
      SymmetricKeyCryptoFormat symmetricKeyCryptoFormat,
      MapSerde serde) {
    this.dekProvider = dekProvider;
    this.kekIdForWriting = kekIdForWriting;
    this.symmetricKeyCryptoFormat = symmetricKeyCryptoFormat;
    this.serde = serde;
  }

  @Override
  public InputFile decrypt(EncryptedInputFile encrypted) {
    EncryptionKeyMetadata encryptionKeyMetadata = encrypted.keyMetadata();
    SerializedConf conf = SerializedConf.of(serde, encryptionKeyMetadata.buffer());

    Dek encryptedDek = Dek.load(conf);
    K kekId = dekProvider.loadKekId(conf);
    Dek plaintextDek = dekProvider.getPlaintextDek(kekId, encryptedDek);
    return new SymmetricKeyDecryptedInputFile(
        encrypted.encryptedInputFile(),
        symmetricKeyCryptoFormat,
        plaintextDek.plaintextDek(),
        plaintextDek.iv());
  }

  @Override
  public EncryptedOutputFile encrypt(OutputFile plaintextOutputFile) {
    Dek dek =
        dekProvider.getNewDek(
            kekIdForWriting,
            symmetricKeyCryptoFormat.dekLength(),
            symmetricKeyCryptoFormat.ivLength());

    OutputFile encryptingOutputFile =
        new SymmetricKeyEncryptedOutputFile(
            plaintextOutputFile, symmetricKeyCryptoFormat, dek.plaintextDek(), dek.iv());
    SerializedConf conf = SerializedConf.empty(serde);
    dek.dump(conf);
    kekIdForWriting.dump(conf);
    BasicEncryptionKeyMetadata basicEncryptionKeyMetadata =
        new BasicEncryptionKeyMetadata(conf.toBytes());

    return new BasicEncryptedOutputFile(encryptingOutputFile, basicEncryptionKeyMetadata);
  }
}
