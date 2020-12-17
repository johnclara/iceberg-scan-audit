package com.box.dataplatform.iceberg.core.metastore;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.box.dataplatform.iceberg.metastore.BaseDynamoMetastore;

public class DynamoDBIcebergTableMetadata {
  private String tableName;
  private String metadataLocation;
  private Long version;

  @DynamoDBHashKey(attributeName = BaseDynamoMetastore.TABLE_NAME_COLUMN_NAME)
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @DynamoDBAttribute(attributeName = BaseDynamoMetastore.METADATA_LOCATION_COLUMN_NAME)
  public String getMetadataLocation() {
    return metadataLocation;
  }

  public void setMetadataLocation(String metadataLocation) {
    this.metadataLocation = metadataLocation;
  }

  @DynamoDBVersionAttribute(attributeName = "version")
  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }
}
