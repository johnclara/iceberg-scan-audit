package com.box.dataplatform.iceberg.metastore;

import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseDynamoMetastore implements Metastore {
  private static final Logger logger = LoggerFactory.getLogger(BaseDynamoMetastore.class);
  private static final int MAX_COMMIT_ATTEMPTS = 1000;

  public static final String TABLE_NAME_COLUMN_NAME = "tableName";
  public static final String METADATA_LOCATION_COLUMN_NAME = "metadataLocation";

  private final TenantDynamoTableMap tenantDynamoTableMap;

  protected BaseDynamoMetastore(TenantDynamoTableMap tenantDynamoTableMap) {
    this.tenantDynamoTableMap = tenantDynamoTableMap;
  }

  protected static class ConcurrentUpdateException extends RuntimeException {
    public ConcurrentUpdateException(String message) {
      super(message);
    }

    public ConcurrentUpdateException(String message, Exception e) {
      super(message, e);
    }
  };

  protected abstract String getMetadataLocationForTable(
      String dynamoTableName, String icebergTableName);

  protected abstract boolean deleteIcebergTable(String dynamoTableName, String icebergTableName);

  protected abstract void atomicUpdateMetadataLocation(
      String dynamoTableName,
      String icebergTableName,
      String oldMetadataLocation,
      String newMetadataLocation)
      throws ConcurrentUpdateException;

  @Override
  public String getMetadataForTable(String tenantName, String tableName) {
    String dynamoTableName = tenantDynamoTableMap.getDynamoTableName(tenantName);
    return getMetadataLocationForTable(dynamoTableName, tableName);
  }

  @Override
  public void updateMetadataLocation(
      String tenantName,
      String icebergTableName,
      String oldMetadataLocation,
      String newMetadataLocation) {
    String dynamoTableName = tenantDynamoTableMap.getDynamoTableName(tenantName);
    int attempt = 0;

    // Based on: BaseMetastoreTableOperations#refreshFromMetadataLocation
    Tasks.foreach(0)
        .retry(MAX_COMMIT_ATTEMPTS)
        .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
        .throwFailureWhenFinished()
        .shouldRetryTest(e -> !(e instanceof ConcurrentUpdateException))
        .run(
            i ->
                atomicUpdateMetadataLocation(
                    dynamoTableName, icebergTableName, oldMetadataLocation, newMetadataLocation));
  }

  @Override
  public boolean deleteTable(String tenantName, String icebergTableName) {
    String dynamoTableName = tenantDynamoTableMap.getDynamoTableName(tenantName);
    return deleteIcebergTable(dynamoTableName, icebergTableName);
  }

  @Override
  public boolean renameTable(String tenantName, String from, String to) {
    throw new UnsupportedOperationException("Table renames are unsupported");
  }
}
