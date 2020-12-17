package com.box.dataplatform.aws.client.sdk2.dynamo;

import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk2.AwsCredentialsFactory;
import com.box.dataplatform.aws.client.sdk2.SdkHttpClientFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoClientFactory {
  public static DynamoDbClient create(AwsClientConfig awsClientConfig) {
    SdkHttpClient sdkHttpClient = SdkHttpClientFactory.create(awsClientConfig);
    return create(awsClientConfig, sdkHttpClient);
  }

  public static DynamoDbClient create(
      AwsClientConfig awsClientConfig, SdkHttpClient sdkHttpClient) {
    AwsCredentialsProvider awsCredentialsProvider =
        AwsCredentialsFactory.create(awsClientConfig, () -> sdkHttpClient);
    return DynamoDbClient.builder()
        .credentialsProvider(awsCredentialsProvider)
        .region(Region.of(awsClientConfig.region()))
        .httpClient(sdkHttpClient)
        .build();
  }
}
