package com.box.dataplatform.iceberg.core.testkit.kms;

import com.amazonaws.services.kms.AWSKMS;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.kms.KmsClientFactory;

public class MockKmsClientFactory extends KmsClientFactory {
  public MockKmsClientFactory() {}

  @Override
  public AWSKMS create(AwsClientConfig awsClientConfig) {
    return new MockKmsClient();
  }
}
