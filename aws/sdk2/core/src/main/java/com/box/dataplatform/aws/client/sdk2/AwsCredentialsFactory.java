package com.box.dataplatform.aws.client.sdk2;

import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.StaticCredentials;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class AwsCredentialsFactory {
  private static final LoadingCache<AwsClientConfig, AwsCredentialsProvider> CACHE =
      Caffeine.newBuilder()
          .softValues()
          .build(
              awsClientConfig ->
                  create(awsClientConfig, () -> SdkHttpClientFactory.getOrCreate(awsClientConfig)));

  public static AwsCredentialsProvider getOrCreate(AwsClientConfig awsClientConfig) {
    return CACHE.get(awsClientConfig);
  }

  private static final int DEFAULT_SESSION_TIMEOUT = 30 * 60;
  private static final boolean DEFAULT_ASYNC_UPDATE = true;

  public static AwsCredentialsProvider create(
      AwsClientConfig awsClientConfig, Supplier<SdkHttpClient> sdkHttpClient) {
    if (awsClientConfig.staticAwsCredentials() == null) {
      return InstanceProfileCredentialsProvider.create();
    }
    StaticCredentialsProvider staticCredentialsProvider =
        createStatic(awsClientConfig.staticAwsCredentials());
    if (awsClientConfig.sessionCredentialsConfig() == null) {
      return staticCredentialsProvider;
    }
    return create(awsClientConfig, staticCredentialsProvider, sdkHttpClient.get());
  }

  public static StaticCredentialsProvider createStatic(StaticCredentials staticCredentials) {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(staticCredentials.accessKey(), staticCredentials.secretKey()));
  }

  public static AwsCredentialsProvider create(
      AwsClientConfig awsClientConfig,
      StaticCredentialsProvider staticCredentialsProvider,
      SdkHttpClient sdkHttpClient) {

    // Create sts client using the IAM user credentials
    StsClient stsClient =
        StsClient.builder()
            .credentialsProvider(staticCredentialsProvider)
            .region(Region.of(awsClientConfig.region()))
            .httpClient(sdkHttpClient)
            .build();

    // Request to assume IAM role
    AssumeRoleRequest roleRequest =
        AssumeRoleRequest.builder()
            .roleArn(awsClientConfig.sessionCredentialsConfig().iamRoleArn())
            .roleSessionName(awsClientConfig.sessionCredentialsConfig().sessionName())
            .durationSeconds(DEFAULT_SESSION_TIMEOUT)
            .build();

    // Return STS assume role credentials provider.
    // This provider has a cache which it updates async when the session is about to expire
    // and always returns valid session credentials for making requests
    return StsAssumeRoleCredentialsProvider.builder()
        .refreshRequest(roleRequest)
        .asyncCredentialUpdateEnabled(DEFAULT_ASYNC_UPDATE)
        .stsClient(stsClient)
        .build();
  }
}
