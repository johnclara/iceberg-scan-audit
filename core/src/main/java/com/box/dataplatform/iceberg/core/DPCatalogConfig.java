package com.box.dataplatform.iceberg.core;

import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.iceberg.AbstractDPCatalog;
import com.box.dataplatform.iceberg.core.metastore.DynamoMetastore;
import com.box.dataplatform.iceberg.encryption.dekprovider.AbstractKmsDekProvider;
import com.box.dataplatform.iceberg.encryption.dekprovider.DelegatingDekProvider;
import com.box.dataplatform.iceberg.encryption.dekprovider.PlaintextDekProvider;
import com.box.dataplatform.s3a.S3aConfig;
import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.Dumpable;
import com.box.dataplatform.util.NestedConf;
import com.box.dataplatform.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DPCatalogConfig implements Dumpable {
  Logger log = LoggerFactory.getLogger(DPCatalogConfig.class);

  /** * PROPERTIES AND DEFAULTS ** */
  public static final String KMS_CLIENT_FACTORY_IMPL = "kms.client.factory.impl";

  public static final String KMS_CLIENT_FACTORY_IMPL_DEFAULT = null;

  public static final String DEFAULT_DEK_PROVIDER_NAME = "dekprovider.default";
  public static final String DEFAULT_DEK_PROVIDER_NAME_DEFAULT = "kms";

  public static final String ENCRYPTED_KEKID_BASE = "dekprovider.write.encrypted.kekid";
  public static final String PLAINTEXT_KEKID_BASE = "dekprovider.write.platintext.kekid";

  /** * PRIVATE VARIABLES ** */
  private final AwsClientConfig awsClientConfig;

  private final S3aConfig s3aConfig;
  private final String dynamoClientFactoryImpl;
  private final String kmsClientFactoryImpl;

  public Map<String, String> getSparkOptions() {
    HashMap<String, String> map = new HashMap<>();
    Properties emptyProps = Properties.of(map);
    dump(emptyProps);

    /*
     * Spark Iceberg will sometimes refresh spark's hadoop conf and merge it with table properties
     * at "hadoop." See {@link org.apache.iceberg.spark.source.Reader#138}
     *
     * <p>Here we set hadoop. properties to reset them when hadoop conf is refreshed
     */
    Properties namespacedHadoopProps =
        Properties.of(String.format("hadoop.%s", Conf.DEFAULT_NAMESPACE), map);
    Properties baseHadoopProps = Properties.of("hadoop.", map);
    s3aConfig.dump(namespacedHadoopProps);
    s3aConfig.setS3aHadoopConfig(baseHadoopProps);
    return map;
  }

  public AbstractDPCatalog getCatalog() {
    return DPCatalog.load(getSparkOptions(), new Configuration());
  }

  /** * CONSTRUCTOR ** */
  protected DPCatalogConfig(
      AwsClientConfig awsClientConfig,
      S3aConfig s3aConfig,
      String dynamoClientFactoryImpl,
      String kmsClientFactoryImpl) {
    this.awsClientConfig = awsClientConfig;
    this.s3aConfig = s3aConfig;
    this.dynamoClientFactoryImpl = dynamoClientFactoryImpl;
    this.kmsClientFactoryImpl = kmsClientFactoryImpl;
  }

  /** * BUILDER ** */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    AwsClientConfig awsClientConfig;

    String kmsKeyId = S3aConfig.S3A_KMS_KEY_ID_DEFAULT;

    String dynamoClientFactoryImpl = DynamoMetastore.DYNAMO_CLIENT_FACTORY_IMPL_DEFAULT;
    String kmsClientFactoryImpl = KMS_CLIENT_FACTORY_IMPL_DEFAULT;

    String s3aClientFactoryImpl = S3aConfig.S3A_S3_CLIENT_FACTORY_IMPL_DEFAULT;
    boolean disableCache = S3aConfig.DISABLE_CACHE_DEFAULT;

    protected Builder() {}

    public Builder setAwsClientConfig(AwsClientConfig awsClientConfig) {
      this.awsClientConfig = awsClientConfig;
      return this;
    }

    public Builder setKmsKeyId(String kmsKeyId) {
      this.kmsKeyId = kmsKeyId;
      return this;
    }

    public Builder setDynamoClientFactoryImpl(String dynamoClientFactoryImpl) {
      this.dynamoClientFactoryImpl = dynamoClientFactoryImpl;
      return this;
    }

    public Builder setKmsClientFactoryImpl(String kmsClientFactoryImpl) {
      this.kmsClientFactoryImpl = kmsClientFactoryImpl;
      return this;
    }

    public Builder setS3aClientFactoryImpl(String s3aClientFactoryImpl) {
      this.s3aClientFactoryImpl = s3aClientFactoryImpl;
      return this;
    }

    public Builder setDisableS3aCache(boolean disableCache) {
      this.disableCache = disableCache;
      return this;
    }

    public DPCatalogConfig build() {
      S3aConfig s3aConfig =
          new S3aConfig(kmsKeyId, s3aClientFactoryImpl, disableCache, awsClientConfig);
      return new DPCatalogConfig(
          awsClientConfig, s3aConfig, dynamoClientFactoryImpl, kmsClientFactoryImpl);
    }
  }

  /** * DUMPER ** */
  public void dump(Conf conf) {
    log.info("Using DPIcebergCatalog V1 config: \n" + toString());
    awsClientConfig.dump(conf);
    s3aConfig.dump(conf);

    if (s3aConfig.kmsKeyId() != null) {
      NestedConf encryptedKekIdConf = NestedConf.of(conf, ENCRYPTED_KEKID_BASE);
      encryptedKekIdConf.setString(DelegatingDekProvider.PROVIDER, AbstractKmsDekProvider.NAME);
      AbstractKmsDekProvider.KmsKekId.of(s3aConfig.kmsKeyId()).dump(encryptedKekIdConf);

      NestedConf plaintextKekIdConf = NestedConf.of(conf, PLAINTEXT_KEKID_BASE);
      plaintextKekIdConf.setString(DelegatingDekProvider.PROVIDER, PlaintextDekProvider.NAME);
    }

    if (dynamoClientFactoryImpl != null) {
      conf.setString(DynamoMetastore.DYNAMO_CLIENT_FACTORY_IMPL, dynamoClientFactoryImpl);
    }

    if (kmsClientFactoryImpl != null) {
      conf.setString(KMS_CLIENT_FACTORY_IMPL, kmsClientFactoryImpl);
    }
  }

  /** * GETTERS ** */
  public AwsClientConfig awsClientConfig() {
    return awsClientConfig;
  }

  public S3aConfig s3aConfig() {
    return s3aConfig;
  }

  public String dynamoClientFactoryImpl() {
    return dynamoClientFactoryImpl;
  }

  public String kmsClientFactoryImpl() {
    return kmsClientFactoryImpl;
  }

  /** * EQUALS HASH CODE ** */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DPCatalogConfig)) return false;
    DPCatalogConfig that = (DPCatalogConfig) o;
    return awsClientConfig.equals(that.awsClientConfig)
        && s3aConfig.equals(that.s3aConfig)
        && Objects.equals(dynamoClientFactoryImpl, that.dynamoClientFactoryImpl)
        && Objects.equals(kmsClientFactoryImpl, that.kmsClientFactoryImpl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(awsClientConfig, s3aConfig, dynamoClientFactoryImpl, kmsClientFactoryImpl);
  }

  /** * TO STRING ** */
  @Override
  public String toString() {
    return "DPCatalogConfig{"
        + ", awsClientConfig="
        + awsClientConfig
        + ", s3aConfig="
        + s3aConfig
        + ", dynamoClientFactoryImpl='"
        + dynamoClientFactoryImpl
        + '\''
        + ", kmsClientFactoryImpl='"
        + kmsClientFactoryImpl
        + '\''
        + '}';
  }
}
