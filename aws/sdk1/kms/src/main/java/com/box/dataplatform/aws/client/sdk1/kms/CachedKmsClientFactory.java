package com.box.dataplatform.aws.client.sdk1.kms;

import com.amazonaws.services.kms.AWSKMS;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CachedKmsClientFactory {
  public static final KmsClientFactory DEFAULT_KMS_CLIENT_FACTORY = new KmsClientFactory();
  private static final Cache<AwsClientConfig, AWSKMS> CACHE =
      Caffeine.newBuilder().softValues().build();

  public static AWSKMS getOrCreate(AwsClientConfig awsClientConfig) {
    return CACHE.get(awsClientConfig, DEFAULT_KMS_CLIENT_FACTORY::create);
  }

  public static AWSKMS getOrCreate(
      AwsClientConfig awsClientConfig, KmsClientFactory kmsClientFactory) {
    return CACHE.get(awsClientConfig, kmsClientFactory::create);
  }
}
