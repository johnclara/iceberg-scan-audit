package com.box.dataplatform.aws.client.sdk1;

import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.SessionCredentialsConfig;
import com.box.dataplatform.aws.client.StaticCredentials;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class AwsCredentialsFactory {
  private static final LoadingCache<AwsClientConfig, AWSCredentialsProvider> CACHE =
      Caffeine.newBuilder().softValues().build(awsClientConfig -> create(awsClientConfig));

  public static AWSCredentialsProvider getOrCreate(AwsClientConfig awsClientConfig) {
    return CACHE.get(awsClientConfig);
  }

  public static AWSCredentialsProvider create(AwsClientConfig awsClientConfig) {
    if (awsClientConfig.staticAwsCredentials() == null) {
      return InstanceProfileCredentialsProvider.getInstance();
    }
    AWSStaticCredentialsProvider staticCredentialsProvider =
        createStatic(awsClientConfig.staticAwsCredentials());
    if (awsClientConfig.sessionCredentialsConfig() == null) {
      return staticCredentialsProvider;
    }
    return createWithRole(awsClientConfig, staticCredentialsProvider);
  }

  public static AWSStaticCredentialsProvider createStatic(StaticCredentials staticCredentials) {
    return new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(staticCredentials.accessKey(), staticCredentials.secretKey()));
  }

  public static STSAssumeRoleSessionCredentialsProvider createWithRole(
      AwsClientConfig awsClientConfig, AWSStaticCredentialsProvider staticCredentialsProvider) {
    AWSSecurityTokenServiceClientBuilder stsClientBuilder =
        AWSSecurityTokenServiceClient.builder()
            .withCredentials(staticCredentialsProvider)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    String.format("sts.%s.amazonaws.com", awsClientConfig.region()),
                    awsClientConfig.region()));

    if (awsClientConfig.proxyConfig() != null) {
      stsClientBuilder =
          stsClientBuilder.withClientConfiguration(
              ClientConfigurationFactory.create(awsClientConfig.proxyConfig()));
    }

    SessionCredentialsConfig sessionCredentialsConfig = awsClientConfig.sessionCredentialsConfig();
    return new STSAssumeRoleSessionCredentialsProvider.Builder(
            sessionCredentialsConfig.iamRoleArn(), sessionCredentialsConfig.sessionName())
        .withStsClient(stsClientBuilder.build())
        .build();
  }
}
