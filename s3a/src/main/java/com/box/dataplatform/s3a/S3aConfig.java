package com.box.dataplatform.s3a;

import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.Dumpable;
import com.box.dataplatform.util.LoadableBuilder;
import java.io.Serializable;
import java.util.Objects;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3aConfig implements Serializable, Dumpable {
  private static final Logger log = LoggerFactory.getLogger(S3aConfig.class);
  /** * PROPERTIES AND DEFAULTS ** */
  public static String S3A_KMS_KEY_ID = "s3a.kms.key.id";

  public static String S3A_KMS_KEY_ID_DEFAULT = null;

  public static String S3A_S3_CLIENT_FACTORY_IMPL = "s3a.s3.client.factory.impl";
  public static String S3A_S3_CLIENT_FACTORY_IMPL_DEFAULT = S3aS3ClientFactory.class.getName();

  public static String DISABLE_CACHE = "s3a.disable.cache";
  public static boolean DISABLE_CACHE_DEFAULT = false;

  /** * PRIVATE VARIABLES ** */
  private final String kmsKeyId;

  private final String s3aS3ClientFactoryImpl;
  private final boolean disableCache;
  private final AwsClientConfig awsClientConfig;

  public void setS3aHadoopConfig(Conf conf) {
    log.info("S3a initializing using config: {}", toString());
    conf.setString(Constants.AWS_CREDENTIALS_PROVIDER, HadoopCredentialsProvider.class.getName());

    for (String scheme : S3aFileSystemCacheKey.allSchemes()) {
      log.info("Setting up scheme {}", scheme);
      conf.setString(String.format("fs.%s.impl", scheme), S3AFileSystem.class.getName());
      conf.setBoolean(String.format("fs.%s.impl.disable.cache", scheme), disableCache);
    }

    if (awsClientConfig.proxyConfig() != null) {
      conf.setString(Constants.PROXY_HOST, awsClientConfig.proxyConfig().host());
      conf.setInt(Constants.PROXY_PORT, awsClientConfig.proxyConfig().port());
      conf.setString(Constants.PROXY_USERNAME, awsClientConfig.proxyConfig().username());
      conf.setString(Constants.PROXY_PASSWORD, awsClientConfig.proxyConfig().password());
    }

    conf.setString(Constants.S3_CLIENT_FACTORY_IMPL, s3aS3ClientFactoryImpl);
  }

  public S3aConfig(
      String kmsKeyId,
      String s3aS3ClientFactoryImpl,
      boolean disableCache,
      AwsClientConfig awsClientConfig) {
    this.kmsKeyId = kmsKeyId;
    this.s3aS3ClientFactoryImpl = s3aS3ClientFactoryImpl;
    this.disableCache = disableCache;
    this.awsClientConfig = awsClientConfig;
  }

  /** * LOADER ** */
  public static S3aConfig load(Conf conf) {
    return new Builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, S3aConfig> {
    public Builder() {}

    private String kmsKeyId;
    private String s3aS3ClientFactoryImpl;
    private boolean disableCache;
    private AwsClientConfig awsClientConfig;

    @Override
    public Builder load(Conf conf) {
      kmsKeyId = conf.propertyAsString(S3A_KMS_KEY_ID, S3A_KMS_KEY_ID_DEFAULT);

      s3aS3ClientFactoryImpl =
          conf.propertyAsString(S3A_S3_CLIENT_FACTORY_IMPL, S3A_S3_CLIENT_FACTORY_IMPL_DEFAULT);

      disableCache = conf.propertyAsBoolean(DISABLE_CACHE, DISABLE_CACHE_DEFAULT);

      awsClientConfig = AwsClientConfig.load(conf);
      return this;
    }

    @Override
    public S3aConfig build() {
      return new S3aConfig(kmsKeyId, s3aS3ClientFactoryImpl, disableCache, awsClientConfig);
    }
  }

  /** * DUMPER ** */
  public void dump(Conf conf) {
    conf.setString(S3A_KMS_KEY_ID, kmsKeyId);
    conf.setString(S3A_S3_CLIENT_FACTORY_IMPL, s3aS3ClientFactoryImpl);
    conf.setBoolean(DISABLE_CACHE, disableCache);
    awsClientConfig.dump(conf);
  }

  /** * GETTERS ** */
  public String kmsKeyId() {
    return kmsKeyId;
  }

  public String s3aS3ClientFactoryImpl() {
    return s3aS3ClientFactoryImpl;
  }

  public boolean disableCache() {
    return disableCache;
  }

  public AwsClientConfig awsClientConfig() {
    return awsClientConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof S3aConfig)) return false;
    S3aConfig s3aConfig = (S3aConfig) o;
    return disableCache == s3aConfig.disableCache
        && Objects.equals(kmsKeyId, s3aConfig.kmsKeyId)
        && s3aS3ClientFactoryImpl.equals(s3aConfig.s3aS3ClientFactoryImpl)
        && awsClientConfig.equals(s3aConfig.awsClientConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kmsKeyId, s3aS3ClientFactoryImpl, disableCache, awsClientConfig);
  }

  @Override
  public String toString() {
    return "S3aConfig{"
        + "kmsKeyId='"
        + kmsKeyId
        + '\''
        + ", s3aS3ClientFactoryImpl='"
        + s3aS3ClientFactoryImpl
        + '\''
        + ", disableCache="
        + disableCache
        + ", awsClientConfig="
        + awsClientConfig
        + '}';
  }
}
