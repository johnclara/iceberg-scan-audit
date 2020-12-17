package com.box.dataplatform.iceberg.client;

import com.amazonaws.AmazonClientException;
import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import com.box.dataplatform.iceberg.client.exceptions.RecordTransformationException;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;

/**
 * An OpenDataFile for constructing an iceberg data file for a given table.
 *
 * <p>All records added to this datafile should be for the same partition and table.
 *
 * <p>LifeCycle: - use a builder to build the OpenDataFile. Builder calls should only be retried for
 * DPIcebergRetryable exceptions.
 *
 * <p>- {@link #start()} the open data file. This will attempt to create an encrypted output file
 * which can throw and all exceptions should be retried. This method is not thread safe.
 *
 * <p>- {@link #add(RecordType)} to the open data file. The actual appends for this method are
 * synchronized so this section is thread safe.
 *
 * <p>- {@link #complete()} the open data file. This will try to create an Iceberg DataFile which
 * can throw and all exceptions should be retried. This method is not thread safe.
 *
 * @param <RecordType> the record type for the data file (eg GenericRecord)
 */
public abstract class OpenDataFileImpl<RecordType> implements OpenDataFile<RecordType> {
  protected FileAppender<RecordType> writer;
  private final EncryptedOutputFile encryptedOutputFile;
  private final PartitionSpec partitionSpec;
  private final StructLike partitionData;
  private final WriterBuilder<RecordType> writerBuilder;
  private Boolean closed;

  /** @param <RecordType> */
  public interface WriterBuilder<RecordType> {
    FileAppender<RecordType> build(EncryptedOutputFile encryptedOutputFile) throws IOException;
  }

  protected OpenDataFileImpl(
      EncryptedOutputFile encryptedOutputFile,
      PartitionSpec partitionSpec,
      StructLike partitionData,
      WriterBuilder<RecordType> writerBuilder) {
    this.encryptedOutputFile = encryptedOutputFile;
    this.partitionSpec = partitionSpec;
    this.partitionData = partitionData;
    this.writerBuilder = writerBuilder;
    this.closed = false;
  }

  protected synchronized void appendToDataFile(RecordType record) throws AWSClientIOException {
    if (writer == null) {
      throw new IllegalStateException("The writer for has not been instantiated");
    }
    writer.add(record);
  }

  public void start() throws DPIcebergRetryableException {
    if (closed) {
      throw new IllegalStateException("The file has already been completed");
    }
    if (writer != null) {
      throw new IllegalStateException("The file has already been started");
    }

    try {
      this.writer = writerBuilder.build(encryptedOutputFile);
    } catch (UncheckedIOException e) {
      if (e.getCause() instanceof AWSClientIOException) {
        throw new DPIcebergRetryableException(
            "Failed to start open data file: " + e.getMessage(), e);
      } else {
        throw new RuntimeException("Failed to start open data file", e);
      }
    } catch (AmazonClientException ace) {
      throw new DPIcebergRetryableException(
          "Failed to start open data file: " + ace.getMessage(), ace);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start open data file", e);
    }
  }

  public abstract void add(RecordType record)
      throws DPIcebergRetryableException, RecordTransformationException;

  private void close() throws DPIcebergRetryableException {
    if (writer != null) {
      try {
        writer.close();
      } catch (AmazonClientException | AWSClientIOException e) {
        throw new DPIcebergRetryableException(
            "Failed to close open data file: " + e.getMessage(), e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    closed = true;
  }

  /**
   * Close should be called before complete
   *
   * @return
   */
  public DataFile complete() throws DPIcebergRetryableException {
    if (!closed) {
      close();
    }
    InputFile inputFile = encryptedOutputFile.encryptingOutputFile().toInputFile();
    DataFiles.Builder builder = new DataFiles.Builder(partitionSpec);
    builder.withPartition(partitionData);
    builder.withEncryptionKeyMetadata(encryptedOutputFile.keyMetadata());
    builder.withFileSizeInBytes(writer.length());
    builder.withPath(inputFile.location());
    // avoid calling s3a status which can lead to java.io.FileNotFoundException because of s3
    // eventual consistency.
    // builder.withEncryptedOutputFile(encryptedOutputFile);
    builder.withMetrics(writer.metrics());
    builder.withSplitOffsets(writer.splitOffsets());
    return builder.build();
  }
}
