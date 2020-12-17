package com.box.dataplatform.iceberg.core.testkit.s3;

import static com.box.dataplatform.iceberg.core.testkit.util.TestkitUtils.getAndCheckProperty;

import com.amazonaws.services.s3.AmazonS3;
import java.io.IOException;
import org.apache.hadoop.fs.Path;

public class S3Testkit {
  public static final String S3_BUCKET = "test";

  /** This will start s3mock and create a test bucket */
  public static void start() {
    String s3mockRootDir = getAndCheckProperty("s3mock.root.dir");
    S3MockContainer.start(s3mockRootDir);

    AmazonS3 s3Client = null;
    try {
      s3Client =
          new MockS3AS3ClientFactory()
              .createS3Client(new Path(String.format("s3a://%s/", S3Testkit.S3_BUCKET)).toUri());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    s3Client.createBucket(S3Testkit.S3_BUCKET);
  }

  public static void stop() {
    S3MockContainer.stop();
  }
}
