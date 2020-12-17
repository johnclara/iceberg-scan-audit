package com.box.dataplatform.iceberg.client.avro;

import com.amazonaws.AmazonClientException;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.iceberg.AbstractDPCatalog;
import com.box.dataplatform.iceberg.DPTableProperties;
import com.box.dataplatform.iceberg.client.*;
import com.box.dataplatform.iceberg.client.avro.transformations.EntitySchemaTransformer;
import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import com.box.dataplatform.iceberg.client.exceptions.RecordTransformationException;
import com.box.dataplatform.iceberg.client.model.PartitionInfo;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionHelper;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/** DataPlatform Iceberg client which creates AvroOpenDataFiles or DataFiles. */
public class DPIcebergAvroClient extends DPIcebergClientImpl<GenericRecord> {
  private final GenericRecordTransformer transformer;

  private final Double loggingPercentage;
  private final Random rand;
  private final long timeMultiplier;
  private static final double DEFAULT_LOGGING_PERCENTAGE = 0.1;

  private GenericRecordTransformer getTransformer(Map<String, String> properties) {
    String writeWithEntityTransformProperty =
        properties.getOrDefault(
            DPTableProperties.WRITE_WITH_ENTITY_TRANSFORM,
            DPTableProperties.WRITE_WITH_ENTITY_TRANSFORM_DEFAULT);
    boolean entityTransform = Boolean.parseBoolean(writeWithEntityTransformProperty);

    if (entityTransform && isPartitioned) {
      throw new IllegalStateException("Partitioned tables with transforms are not supported");
    }

    if (entityTransform) {
      return new EntitySchemaTransformer();
    }

    return null;
  }

  private Double getLoggingPercentage() {
    String loggingPercentageValue = System.getenv("DP_ICEBERG_LOGGING_PERCENTAGE");
    if (loggingPercentageValue == null) {
      return DEFAULT_LOGGING_PERCENTAGE;
    } else {
      return Double.parseDouble(loggingPercentageValue);
    }
  }

  private static long getMultiplier(TimeUnit timeUnit) {
    if (timeUnit == TimeUnit.MILLISECONDS) return 1000L;
    if (timeUnit == TimeUnit.SECONDS) return 1000000L;
    if (timeUnit == TimeUnit.MICROSECONDS) return 1;
    else throw new IllegalArgumentException("Unsupported time unit " + timeUnit.name());
  }

  public DPIcebergAvroClient(
      String tenantName,
      String tableName,
      AwsClientConfig awsClientConfig,
      String kmsKeyId,
      TimeUnit timeUnit,
      boolean useFastAppend) {
    super(tenantName, tableName, awsClientConfig, kmsKeyId, useFastAppend);
    loggingPercentage = getLoggingPercentage();
    transformer = getTransformer(table.properties());
    rand = new Random();
    timeMultiplier = getMultiplier(timeUnit);
  }

  public DPIcebergAvroClient(
      String tenantName,
      String tableName,
      AbstractDPCatalog abstractDpCatalog,
      TimeUnit timeUnit,
      boolean useFastAppend) {
    super(TableIdentifier.of(tenantName, tableName), abstractDpCatalog, useFastAppend);
    loggingPercentage = getLoggingPercentage();
    transformer = getTransformer(table.properties());
    rand = new Random();
    timeMultiplier = getMultiplier(timeUnit);
  }

  @VisibleForTesting
  protected DPIcebergAvroClient(
      TableIdentifier tableIdentifier, Table table, long timeMultiplier, boolean useFastAppend) {
    super(tableIdentifier, table, useFastAppend);
    loggingPercentage = getLoggingPercentage();
    transformer = getTransformer(table.properties());
    rand = new Random();
    this.timeMultiplier = timeMultiplier;
  }

  @Override
  public OpenDataFile<GenericRecord> createOpenDataFile(List<GenericRecord> records)
      throws DPIcebergRetryableException {
    if (records.size() == 0) {
      throw new IllegalArgumentException("cannot process empty batch");
    }
    AvroOpenDataFile.Builder openDataFileBuilder = AvroOpenDataFile.builder(tableIdentifier, table);
    openDataFileBuilder.withTransformer(transformer);
    return openDataFileBuilder.fromRecord(records.get(0));
  }

  /**
   * This method will mutate the GenericRecord in place to match Iceberg partitioning.
   *
   * @param record an untransformed input record
   * @return the partition info of the record
   */
  public PartitionInfo getPartitionForRecord(GenericRecord record) {
    // Because the table can only be partitioned without transformation,
    // it is okay to expect the partition fields to be in the untransformed record
    PartitionTimestampMutator.mutatePartitionTimestampFields(table, record, timeMultiplier);
    return getPartitionForRecordInternal(record);
  }

  /**
   * This method will not mutate the generic record.
   *
   * @param record an untransformed input record
   * @return
   */
  protected PartitionInfo getPartitionForRecordInternal(GenericRecord record) {
    StructLike partitionData = PartitionHelper.getPartitionData(table.spec(), record);
    return new PartitionInfo(partitionData);
  }

  /**
   * process batch of generic avro records, assumes that every record in batch has gone through
   * {@link DPIcebergClientImpl ::getPartitionForRecord} which mutates record with compatible time
   * stamp.
   *
   * @throws DPIcebergRetryableException when if any aws client exception is thrown to indicate
   *     retry assuming its intermitten failure.
   * @param records
   * @return
   */
  @Override
  public DataFile processBatch(List<GenericRecord> records) {
    OpenDataFile<GenericRecord> openDataFile = createOpenDataFile(records);

    if (openDataFile == null) {
      throw new IllegalArgumentException("All records are invalid.");
    }

    DataFile result;
    try {
      openDataFile.start();
      for (GenericRecord record : records) {
        try {
          openDataFile.add(record);
        } catch (RecordTransformationException e) {
          if (rand.nextDouble() < (loggingPercentage / 100)) {
            transformer.logFailedRecord(tableIdentifier.toString(), record, e);
          }
        }
      }
    } catch (AmazonClientException ace) {
      throw new DPIcebergRetryableException("failed to process batch: " + ace.getMessage(), ace);
    } finally {
      result = openDataFile.complete();
    }
    return result;
  }
}
