package com.box.dataplatform.aws.client.sdk1.kms;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.AwsCredentialsFactory;
import com.box.dataplatform.aws.client.sdk1.ClientConfigurationFactory;
import java.io.Serializable;

public class KmsClientFactory implements Serializable {

  /** Empty public constructor required for reflection */
  public KmsClientFactory() {}

  public AWSKMS create(AwsClientConfig awsClientConfig) {
    AWSKMSClientBuilder awskmsClientBuilder =
        AWSKMSClientBuilder.standard()
            .withCredentials(AwsCredentialsFactory.getOrCreate(awsClientConfig));

    if (awsClientConfig.proxyConfig() != null) {
      awskmsClientBuilder =
          awskmsClientBuilder.withClientConfiguration(
              ClientConfigurationFactory.create(awsClientConfig.proxyConfig()));
    }

    return awskmsClientBuilder.build();
  }
}
