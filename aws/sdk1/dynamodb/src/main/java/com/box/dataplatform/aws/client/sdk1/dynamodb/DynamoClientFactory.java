package com.box.dataplatform.aws.client.sdk1.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.AwsCredentialsFactory;
import com.box.dataplatform.aws.client.sdk1.ClientConfigurationFactory;

public class DynamoClientFactory {

  /** Empty public constructor required for reflection */
  public DynamoClientFactory() {}

  public AmazonDynamoDB create(AwsClientConfig awsClientConfig) {
    AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder =
        AmazonDynamoDBClientBuilder.standard()
            .withRegion(awsClientConfig.region())
            .withCredentials(AwsCredentialsFactory.getOrCreate(awsClientConfig));

    if (awsClientConfig.proxyConfig() != null) {
      amazonDynamoDBClientBuilder =
          amazonDynamoDBClientBuilder.withClientConfiguration(
              ClientConfigurationFactory.create(awsClientConfig.proxyConfig()));
    }

    return amazonDynamoDBClientBuilder.build();
  }
}
