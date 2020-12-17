package com.box.dataplatform.aws.client.sdk1.s3;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.AwsCredentialsFactory;
import com.box.dataplatform.aws.client.sdk1.ClientConfigurationFactory;
import com.box.dataplatform.crypto.BCEncryptor;

/** */
public class S3ClientFactory {
  public static AmazonS3 create(AwsClientConfig awsClientConfig) {
    AmazonS3ClientBuilder s3ClientBuilder =
        AmazonS3ClientBuilder.standard()
            .withCredentials(AwsCredentialsFactory.getOrCreate(awsClientConfig));

    if (awsClientConfig.proxyConfig() != null) {
      s3ClientBuilder =
          s3ClientBuilder.withClientConfiguration(
              ClientConfigurationFactory.create(awsClientConfig.proxyConfig()));
    }
    return s3ClientBuilder.build();
  }

  // TODO check if useInstructionFile is used
  public static AmazonS3 createEncryptedClient(
      AwsClientConfig awsClientConfig, String kmsKeyId, boolean useInstructionFile) {
    KMSEncryptionMaterialsProvider encryptionMaterialsProvider =
        new KMSEncryptionMaterialsProvider(kmsKeyId);

    CryptoConfiguration cryptoConfiguration =
        new CryptoConfiguration()
            .withSecureRandom(BCEncryptor.INSTANCE.secureRandom())
            .withCryptoProvider(BCEncryptor.INSTANCE.provider())
            .withAlwaysUseCryptoProvider(true)
            .withAwsKmsRegion(RegionUtils.getRegion(awsClientConfig.region()))
            .withCryptoMode(CryptoMode.AuthenticatedEncryption);

    AmazonS3EncryptionClientBuilder s3ClientBuilder =
        AmazonS3EncryptionClientBuilder.standard()
            .withCryptoConfiguration(cryptoConfiguration)
            .withCredentials(AwsCredentialsFactory.getOrCreate(awsClientConfig))
            .withRegion(awsClientConfig.region());

    if (awsClientConfig.proxyConfig() != null) {
      s3ClientBuilder =
          s3ClientBuilder.withClientConfiguration(
              ClientConfigurationFactory.create(awsClientConfig.proxyConfig()));
    }

    return s3ClientBuilder.build();
  }
}
