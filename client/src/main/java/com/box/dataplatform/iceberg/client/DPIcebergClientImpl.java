package com.box.dataplatform.iceberg.client;

import com.amazonaws.AmazonClientException;
import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.iceberg.AbstractDPCatalog;
import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import com.box.dataplatform.iceberg.client.exceptions.InvalidCommitException;
import com.box.dataplatform.iceberg.client.model.PartitionInfo;
import com.box.dataplatform.iceberg.core.DPCatalogConfig;
import com.google.common.annotations.VisibleForTesting;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class to create iceberg clientto be able to write data from outside aws. note that this
 * implementation is not using hidden partitioning scheme. DSE is going to use event time and write
 * to respective daily partition.
 *
 * @param <RecordType> the record type of the client (eg GenericRecord)
 */
public abstract class DPIcebergClientImpl<RecordType> implements DPIcebergClient<RecordType> {
  protected final TableIdentifier tableIdentifier;
  protected final Table table;
  protected final boolean isPartitioned;
  private final boolean useFastAppend;
  protected final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

  @VisibleForTesting
  protected DPIcebergClientImpl(
      TableIdentifier tableIdentifier, Table table, boolean useFastAppend) {
    this.tableIdentifier = tableIdentifier;
    this.table = table;
    this.isPartitioned = !table.spec().fields().isEmpty();
    this.useFastAppend = useFastAppend;
  }

  @VisibleForTesting
  protected DPIcebergClientImpl(
      TableIdentifier tableIdentifier, AbstractDPCatalog catalog, boolean useFastAppend) {
    this(tableIdentifier, catalog.loadTable(tableIdentifier), useFastAppend);
  }

  public DPIcebergClientImpl(
      String tenantName,
      String tableName,
      AwsClientConfig awsClientConfig,
      String kmsKeyId,
      boolean useFastAppend) {
    this(
        TableIdentifier.of(tenantName, tableName),
        DPCatalogConfig.builder()
            .setAwsClientConfig(awsClientConfig)
            .setKmsKeyId(kmsKeyId)
            .build()
            .getCatalog(),
        useFastAppend);
  }

  public abstract OpenDataFile<RecordType> createOpenDataFile(List<RecordType> firstRecord)
      throws DPIcebergRetryableException;

  public abstract PartitionInfo getPartitionForRecord(RecordType record);

  public abstract DataFile processBatch(List<RecordType> records);

  /**
   * commit datafiles to iceberg as new snapshot.
   *
   * @throws DPIcebergRetryableException when CommitFailedException exception is thrown by iceberg
   *     so that commit can be retried.
   * @param datafiles
   */
  public void commitBatch(List<DataFile> datafiles)
      throws DPIcebergRetryableException, InvalidCommitException {
    AppendFiles appendFiles;
    if (useFastAppend) {
      appendFiles = table.newFastAppend();
    } else {
      appendFiles = table.newAppend();
    }
    datafiles.forEach(file -> appendFiles.appendFile(file));
    try {
      appendFiles.commit();
    } catch (UncheckedIOException e) {
      if (e.getCause() instanceof AWSClientIOException) {
        throw new DPIcebergRetryableException("Failed commit: " + e.getMessage(), e);
      } else {
        throw new RuntimeException("Failed commit", e);
      }
    } catch (CommitFailedException | AmazonClientException e) {
      throw new DPIcebergRetryableException("failed commit.", e);
    } catch (ValidationException e) {
      throw new InvalidCommitException(e);
    }
  }
}
