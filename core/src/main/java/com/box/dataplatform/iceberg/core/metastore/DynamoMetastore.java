package com.box.dataplatform.iceberg.core.metastore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.sdk1.dynamodb.CachedDynamoClientFactory;
import com.box.dataplatform.aws.client.sdk1.dynamodb.DynamoClientFactory;
import com.box.dataplatform.iceberg.metastore.BaseDynamoMetastore;
import com.box.dataplatform.iceberg.metastore.BoxTenantDynamoTableMap;
import com.box.dataplatform.iceberg.util.ReflectionUtil;
import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.LoadableBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoMetastore extends BaseDynamoMetastore {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String DYNAMO_CLIENT_FACTORY_IMPL = "dynamo.client.factory.impl";

  public static final String DYNAMO_CLIENT_FACTORY_IMPL_DEFAULT = null;

  /** * LOGGERS & PRIVATE CONSTANTS ** */
  private static final Logger log = LoggerFactory.getLogger(DynamoMetastore.class);

  /** * PRIVATE VARIABLES ** */
  private final AwsClientConfig awsClientConfig;

  private final DynamoClientFactory dynamoClientFactory;
  private transient DynamoDBMapper mapper;

  /** * CONSTRUCTOR ** */
  public DynamoMetastore(AwsClientConfig awsClientConfig, DynamoClientFactory dynamoClientFactory) {
    super(BoxTenantDynamoTableMap.INSTANCE);
    this.awsClientConfig = awsClientConfig;
    this.dynamoClientFactory = dynamoClientFactory;
  }

  /** * LOGIC ** */
  private DynamoDBMapper getMapper() {
    if (mapper == null) {
      AmazonDynamoDB amazonDynamoDB =
          CachedDynamoClientFactory.getOrCreate(awsClientConfig, dynamoClientFactory);
      mapper = new DynamoDBMapper(amazonDynamoDB);
    }
    return mapper;
  }

  private DynamoDBMapperConfig getMapperConfig(String dynamoTableName) {
    return new DynamoDBMapperConfig.Builder()
        .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE)
        .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
        .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(dynamoTableName))
        .build();
  }

  @Override
  protected String getMetadataLocationForTable(String dynamoTableName, String icebergTableName) {
    DynamoDBMapperConfig mapperConfig = getMapperConfig(dynamoTableName);
    DynamoDBIcebergTableMetadata dynamoRow =
        getMapper().load(DynamoDBIcebergTableMetadata.class, icebergTableName, mapperConfig);
    if (dynamoRow == null) {
      return null;
    } else {
      return dynamoRow.getMetadataLocation();
    }
  }

  @Override
  protected boolean deleteIcebergTable(String dynamoTableName, String icebergTableName) {
    DynamoDBMapperConfig mapperConfig = getMapperConfig(dynamoTableName);
    DynamoDBIcebergTableMetadata dynamoRow =
        getMapper().load(DynamoDBIcebergTableMetadata.class, icebergTableName, mapperConfig);
    if (dynamoRow == null) {
      log.warn(
          String.format(
              "Can't delete iceberg table: %s from dynamo table: %s because it doesn't exist",
              dynamoTableName, icebergTableName));
      return false;
    } else {
      log.warn(
          String.format(
              "deleting iceberg table: %s from dynamo table: %s",
              dynamoTableName, icebergTableName));
      getMapper().delete(dynamoRow, mapperConfig);
      return true;
    }
  }

  @Override
  protected void atomicUpdateMetadataLocation(
      String dynamoTableName,
      String icebergTableName,
      String oldMetadataLocation,
      String newMetadataLocation)
      throws ConcurrentUpdateException {
    DynamoDBMapperConfig mapperConfig = getMapperConfig(dynamoTableName);
    DynamoDBIcebergTableMetadata dynamoRow =
        getMapper().load(DynamoDBIcebergTableMetadata.class, icebergTableName, mapperConfig);
    if (dynamoRow == null) {
      dynamoRow = new DynamoDBIcebergTableMetadata();
      dynamoRow.setTableName(icebergTableName);
      dynamoRow.setMetadataLocation(oldMetadataLocation);
    }

    ExpectedAttributeValue metadataLocationExpectedValue;
    if (oldMetadataLocation == null) {
      metadataLocationExpectedValue = new ExpectedAttributeValue().withExists(false);
    } else {
      metadataLocationExpectedValue =
          new ExpectedAttributeValue(new AttributeValue(oldMetadataLocation)).withExists(true);
    }

    DynamoDBSaveExpression saveExpression =
        new DynamoDBSaveExpression()
            .withExpectedEntry(
                BaseDynamoMetastore.METADATA_LOCATION_COLUMN_NAME, metadataLocationExpectedValue);

    dynamoRow.setMetadataLocation(newMetadataLocation);

    try {
      getMapper().save(dynamoRow, saveExpression, mapperConfig);
    } catch (ConditionalCheckFailedException e) {
      throw new ConcurrentUpdateException("Concurrent snapshot update", e);
    }
  }

  /** * BUILDER ** */
  public static DynamoMetastore load(Conf conf) {
    return builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, DynamoMetastore> {
    private AwsClientConfig awsClientConfig;
    private DynamoClientFactory dynamoClientFactory;

    public Builder() {}

    @Override
    public Builder load(Conf conf) {
      this.awsClientConfig = AwsClientConfig.load(conf);
      log.info("Using client config \n " + awsClientConfig);
      if (!conf.containsKey(DYNAMO_CLIENT_FACTORY_IMPL)) {
        log.info("Using default dynamo client factory");
        dynamoClientFactory = CachedDynamoClientFactory.DEFAULT_DYNAMO_CLIENT_FACTORY;
      } else {
        dynamoClientFactory =
            ReflectionUtil.initializeNoArgConstructor(
                conf.propertyAsString(DYNAMO_CLIENT_FACTORY_IMPL));
      }
      return this;
    }

    @Override
    public DynamoMetastore build() {
      return new DynamoMetastore(awsClientConfig, dynamoClientFactory);
    }
  }
}
