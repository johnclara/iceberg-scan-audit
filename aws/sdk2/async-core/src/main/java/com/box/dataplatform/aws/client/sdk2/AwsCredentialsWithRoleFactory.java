package com.box.dataplatform.aws.client.sdk2;

// TODO the StsAssumeRoleCredentialsProvider doesn't support the async client yet.
public class AwsCredentialsWithRoleFactory {
  private static final int DEFAULT_SESSION_TIMEOUT = 30 * 60;
  private static final boolean DEFAULT_ASYNC_UPDATE = true;

  /*
      public static AwsCredentialsProvider create(AwsClientConfig awsClientConfig) {
          SdkAsyncHttpClient sdkAsyncHttpClient = null;
          if (awsClientConfig.proxyConfig() != null) {
              sdkAsyncHttpClient = SdkAsyncHttpClientFactory.create(awsClientConfig.proxyConfig().get());
          }
          return create(awsClientConfig.awsCredentialsConfig(), sdkAsyncHttpClient);
      }

      public static AwsCredentialsProvider create(SessionCredentialsConfig awsCredentials, SdkAsyncHttpClient sdkHttpClient) {
          StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                      awsCredentials.staticAwsCredentials().accessKey(),
                      awsCredentials.staticAwsCredentials().secretKey()
                )
          );

          // Create sts client using the IAM user credentials
          StsAsyncClient stsClient = StsAsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsCredentials.stsRegion()))
                .httpClient(sdkHttpClient)
                .build();

          // Request to assume IAM role
          AssumeRoleRequest roleRequest = AssumeRoleRequest
                .builder()
                .roleArn(awsCredentials.iamRoleArn())
                .roleSessionName(awsCredentials.sessionName())
                .durationSeconds(DEFAULT_SESSION_TIMEOUT)
                .build();

          // Return STS assume role credentials provider.
          // This provider has a cache which it updates async when the session is about to expire
          // and always returns valid session credentials for making requests

          throw new UnsupportedOperationException();
          return StsAssumeRoleCredentialsProvider
                .builder()
                .refreshRequest(roleRequest)
                .asyncCredentialUpdateEnabled(DEFAULT_ASYNC_UPDATE)
                .stsClient(stsClient)
                .build();
      }
  */
}
