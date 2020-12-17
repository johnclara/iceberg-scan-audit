package com.box.dataplatform.aws.client.sdk1.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CachedDynamoClientFactory {
  public static final DynamoClientFactory DEFAULT_DYNAMO_CLIENT_FACTORY = new DynamoClientFactory();

  private static final Cache<AwsClientConfig, AmazonDynamoDB> CACHE =
      Caffeine.newBuilder().softValues().build();

  public static AmazonDynamoDB getOrCreate(AwsClientConfig awsClientConfig) {
    return CACHE.get(awsClientConfig, DEFAULT_DYNAMO_CLIENT_FACTORY::create);
  }

  public static AmazonDynamoDB getOrCreate(
      AwsClientConfig awsClientConfig, DynamoClientFactory dynamoClientFactory) {
    return CACHE.get(awsClientConfig, dynamoClientFactory::create);
  }
}
