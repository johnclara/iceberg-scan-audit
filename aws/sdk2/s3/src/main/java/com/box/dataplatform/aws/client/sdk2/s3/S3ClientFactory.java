package com.box.dataplatform.aws.client.sdk2.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientFactory {
  public static S3Client create(
      String region, SdkHttpClient sdkHttpClient, AwsCredentialsProvider credentialsProvider) {
    return S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(credentialsProvider)
        .httpClient(sdkHttpClient)
        .build();
  }
}
