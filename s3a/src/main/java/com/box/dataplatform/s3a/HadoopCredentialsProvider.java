package com.box.dataplatform.s3a;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.AwsCredentialsFactory;
import org.apache.hadoop.conf.Configuration;

public class HadoopCredentialsProvider implements AWSCredentialsProvider {
  private final AwsClientConfig awsClientConfig;

  public HadoopCredentialsProvider(Configuration conf) {
    awsClientConfig = AwsClientConfig.load(HadoopConf.of(conf));
  }

  @Override
  public AWSCredentials getCredentials() {
    return AwsCredentialsFactory.getOrCreate(awsClientConfig).getCredentials();
  }

  @Override
  public void refresh() {
    AwsCredentialsFactory.getOrCreate(awsClientConfig).refresh();
  }
}
