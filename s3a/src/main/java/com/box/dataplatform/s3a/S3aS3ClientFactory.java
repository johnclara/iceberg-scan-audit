package com.box.dataplatform.s3a;

import com.amazonaws.services.s3.AmazonS3;
import com.box.dataplatform.aws.client.sdk1.s3.S3ClientFactory;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.S3aS3ClientFactoryShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3aS3ClientFactory extends Configured implements S3aS3ClientFactoryShim {
  private static final Logger log = LoggerFactory.getLogger(S3aS3ClientFactory.class);

  @Override
  public AmazonS3 createS3Client(URI uri) throws IOException {
    String scheme = uri.getScheme();
    S3aFileSystemCacheKey s3AFilesystemCacheKey = S3aFileSystemCacheKey.fromScheme(scheme);
    if (s3AFilesystemCacheKey == null) {
      throw new IllegalArgumentException(
          "This scheme is not supported: " + uri.getHost() + "  scheme: " + uri.getScheme());
    }

    S3aConfig s3aConfig = S3aConfig.load(HadoopConf.of(this.getConf()));

    if (!s3AFilesystemCacheKey.clientSideEncrypted()) {
      return S3ClientFactory.create(s3aConfig.awsClientConfig());
    } else {
      return S3ClientFactory.createEncryptedClient(
          s3aConfig.awsClientConfig(),
          s3aConfig.kmsKeyId(),
          s3AFilesystemCacheKey.useInstructionFile());
    }
  }
}
