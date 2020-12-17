package com.box.dataplatform.iceberg.core.encryption.dekprovider;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;
import com.amazonaws.services.kms.model.GenerateDataKeyRequest;
import com.amazonaws.services.kms.model.GenerateDataKeyResult;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.kms.CachedKmsClientFactory;
import com.box.dataplatform.aws.client.sdk1.kms.KmsClientFactory;
import com.box.dataplatform.iceberg.encryption.dekprovider.AbstractKmsDekProvider;
import com.box.dataplatform.iceberg.util.ReflectionUtil;
import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.LoadableBuilder;
import java.nio.ByteBuffer;

public class KmsDekProvider extends AbstractKmsDekProvider {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String KMS_CLIENT_FACTORY_IMPL = "kms.client.factory.impl";

  /** * LOGGERS & PRIVATE CONSTANTS ** */
  private static final String CUSTOMER_MASTER_KEY_ID = "kms_cmk_id";

  private static final String KEY_SPEC = "AES_256";

  /** * PRIVATE VARIABLES ** */
  private final KmsClientFactory kmsClientFactory;

  private final AwsClientConfig awsClientConfig;

  private transient AWSKMS kms;

  public KmsDekProvider(AwsClientConfig awsClientConfig, KmsClientFactory kmsClientFactory) {
    this.awsClientConfig = awsClientConfig;
    this.kmsClientFactory = kmsClientFactory;
  }

  /** * LOGIC ** */
  private AWSKMS getKms() {
    if (kms == null) {
      kms = CachedKmsClientFactory.getOrCreate(awsClientConfig, kmsClientFactory);
    }
    return kms;
  }

  @Override
  protected KmsGenerateDekResponse getDekFromKms(KmsKekId kmsKekId, int numBytes) {
    GenerateDataKeyRequest request =
        new GenerateDataKeyRequest()
            .withKeyId(kmsKekId.kmsKeyId())
            .withKeySpec(KEY_SPEC)
            .addEncryptionContextEntry(CUSTOMER_MASTER_KEY_ID, kmsKekId.kmsKeyId());

    GenerateDataKeyResult response = getKms().generateDataKey(request);

    return new KmsGenerateDekResponse(
        response.getCiphertextBlob().array(), response.getPlaintext().array());
  }

  @Override
  protected KmsDecryptResponse getPlaintextFromKms(KmsKekId kmsKekId, byte[] encryptedDek) {
    DecryptRequest decryptRequest =
        new DecryptRequest()
            .withCiphertextBlob(ByteBuffer.wrap(encryptedDek))
            .addEncryptionContextEntry(CUSTOMER_MASTER_KEY_ID, kmsKekId.kmsKeyId());

    DecryptResult decrypt = getKms().decrypt(decryptRequest);

    return new KmsDecryptResponse(decrypt.getPlaintext().array());
  }

  /** * BUILDER ** */
  public static KmsDekProvider load(Conf conf) {
    return new Builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, KmsDekProvider> {
    private KmsClientFactory kmsClientFactory;
    private AwsClientConfig awsClientConfig;

    @Override
    public Builder load(Conf conf) {
      awsClientConfig = AwsClientConfig.load(conf);
      if (!conf.containsKey(KMS_CLIENT_FACTORY_IMPL)) {
        kmsClientFactory = CachedKmsClientFactory.DEFAULT_KMS_CLIENT_FACTORY;
      } else {
        kmsClientFactory =
            ReflectionUtil.initializeNoArgConstructor(
                conf.propertyAsString(KMS_CLIENT_FACTORY_IMPL));
      }
      return this;
    }

    @Override
    public KmsDekProvider build() {
      return new KmsDekProvider(awsClientConfig, kmsClientFactory);
    }
  }
}
