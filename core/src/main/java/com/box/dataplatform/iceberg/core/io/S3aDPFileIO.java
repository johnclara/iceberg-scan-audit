package com.box.dataplatform.iceberg.core.io;

import com.box.dataplatform.iceberg.io.DPFileIO;
import com.box.dataplatform.s3a.HadoopConf;
import com.box.dataplatform.s3a.S3aConfig;
import com.box.dataplatform.util.Properties;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;

public class S3aDPFileIO extends HadoopFileIO implements DPFileIO {
  private static final String S3A_SCHEME_URI = "s3a://";
  private static final String META_SCHEME_URI = "meta://";

  /**
   * 'snap' is the default prefix for manifest lists.
   *
   * <p>{@link org.apache.iceberg.SnapshotProducer#manifestListPath()}
   */
  private static final String SNAPSHOT_PREFIX = "snap";

  /**
   * 'metadata.json' is the default suffix for a snapshot's TableMetadata file.
   *
   * <p>It can also end with '.gz', but we don't have any zipped.
   *
   * <p>{@link org.apache.iceberg.TableMetadataParser#fromFileName()}
   */
  private static final String METADATA_SUFFIX = "metadata.json";

  private final Configuration configuration;

  public S3aDPFileIO(Configuration configuration) {
    super(configuration);
    this.configuration = configuration;
  }

  public void initialize(Map<String, String> options) {
    S3aConfig s3aConfig = S3aConfig.load(Properties.of(options));

    // Set box config for hadoop at the normal namespace
    HadoopConf hadoopConf = HadoopConf.of(configuration);
    s3aConfig.dump(hadoopConf);

    HadoopConf baseConf = HadoopConf.of("", configuration);
    // Set s3a specific config at the base namespace
    s3aConfig.setS3aHadoopConfig(baseConf);
  }

  @Override
  public String addHintsForMetadataLocation(
      String fileName, String originalMetadataLocation, boolean encryptManifest) {
    if (encryptManifest
        && !fileName.startsWith(SNAPSHOT_PREFIX)
        && !fileName.endsWith(METADATA_SUFFIX)) {
      return originalMetadataLocation.replaceFirst(S3A_SCHEME_URI, META_SCHEME_URI);
    } else {
      return originalMetadataLocation;
    }
  }
}
