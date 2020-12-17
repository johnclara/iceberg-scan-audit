package com.box.dataplatform.iceberg.core.testkit;

import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.StaticCredentials;
import com.box.dataplatform.iceberg.AbstractDPCatalog;
import com.box.dataplatform.iceberg.core.DPCatalogConfig;
import com.box.dataplatform.iceberg.core.testkit.dynamo.DynamoTestkit;
import com.box.dataplatform.iceberg.core.testkit.kms.MockKmsClientFactory;
import com.box.dataplatform.iceberg.core.testkit.s3.MockS3AS3ClientFactory;
import com.box.dataplatform.iceberg.core.testkit.s3.S3Testkit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DPIcebergTestkit {
  private static final Logger log = LoggerFactory.getLogger(DPIcebergTestkit.class);

  public static final String TENANT_NAME = "audit-log";
  public static final String ICEBERG_DYNAMO_TABLE = "AuditLogIcebergMetadata";
  public static final String KMS_KEY_ID = "test-kms-key";
  public static final String REGION = AwsClientConfig.DEFAULT_REGION;

  public static String uniqueTableName(String tableNamePrefix) {
    String tableGuid = UUID.randomUUID().toString().replaceAll("-", "_");
    return tableNamePrefix + "_" + tableGuid;
  }

  public static TableIdentifier asId(String tableName) {
    return TableIdentifier.of(TENANT_NAME, tableName);
  }

  public static Table createTable(String tableName, Schema schema) {
    Map<String, String> props = new HashMap<>();
    return createTable(tableName, schema, PartitionSpec.builderFor(schema).build(), props);
  }

  public static Table createTable(
      String tableName, Schema schema, PartitionSpec partitionSpec, Map<String, String> props) {
    TableIdentifier tableIdentifier = TableIdentifier.of(DPIcebergTestkit.TENANT_NAME, tableName);
    AbstractDPCatalog catalog = DPIcebergTestkit.getCatalog();
    String tableLocation =
        "s3a://" + S3Testkit.S3_BUCKET + "/" + DPIcebergTestkit.TENANT_NAME + ".db/" + tableName;

    props = new HashMap<>(props); // incase an immutable map is passed in

    // we only need to set this during test to avoid error reported for metadata directory
    props.put(TableProperties.WRITE_METADATA_LOCATION, tableLocation + "/meta_test");

    Table table = catalog.createTable(tableIdentifier, schema, partitionSpec, tableLocation, props);

    Table loadedTable = catalog.loadTable(tableIdentifier);

    return table;
  }

  public static Table getTable(String tableName) {
    return DPIcebergTestkit.getCatalog()
        .loadTable(TableIdentifier.of(DPIcebergTestkit.TENANT_NAME, tableName));
  }

  public static void start() {
    log.info("Testkit starting");

    log.info("Refreshing mock dynamo");
    DynamoTestkit.refresh();

    log.info("Refreshing mock s3");
    S3Testkit.start();

    log.info("Testkit started");
  }

  public static void stop() {
    log.info("Testkit stopping");

    DynamoTestkit.stop();

    S3Testkit.stop();
  }

  public static DPCatalogConfig getConfig() {
    return DPCatalogConfig.builder()
        .setAwsClientConfig(
            AwsClientConfig.builder()
                .setRegion(REGION)
                .setStaticCredentials(new StaticCredentials("foo", "bar"))
                .build())
        .setKmsKeyId(KMS_KEY_ID)
        .setDisableS3aCache(true)
        .setKmsClientFactoryImpl(MockKmsClientFactory.class.getName())
        .setDynamoClientFactoryImpl(DynamoTestkit.class.getName())
        .setS3aClientFactoryImpl(MockS3AS3ClientFactory.class.getName())
        .build();
  }

  public static AbstractDPCatalog getCatalog() {
    return getConfig().getCatalog();
  }

  public static Map<String, String> getOptions() {
    return getConfig().getSparkOptions();
  }
}
