package com.box.dataplatform.iceberg.client.avro;

import static org.apache.iceberg.FileFormat.AVRO;

import com.amazonaws.AmazonClientException;
import com.box.dataplatform.iceberg.client.OpenDataFileImpl;
import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import com.box.dataplatform.iceberg.client.exceptions.RecordTransformationException;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.s3a.AWSClientIOException;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.TypeUtil;

/**
 * An {@link OpenDataFileImpl} for writing avro records to an iceberg table.
 *
 * <p>{@link #add(GenericRecord)} will also perform record transformations on the records. If a
 * record transformation throws for any reason, this will throw a RecordTransformationException.
 */
public class AvroOpenDataFile extends OpenDataFileImpl<GenericRecord> {
  private final Schema schema;
  private final GenericRecordTransformer transformer;

  protected AvroOpenDataFile(
      Schema schema,
      StructLike partitionData,
      PartitionSpec partitionSpec,
      EncryptedOutputFile encryptedOutputFile,
      org.apache.iceberg.Schema reassignIdSchema,
      Map<String, String> tableProperties,
      GenericRecordTransformer transformer) {
    super(
        encryptedOutputFile,
        partitionSpec,
        partitionData,
        (file) ->
            Avro.write(file.encryptingOutputFile())
                .schema(reassignIdSchema)
                .setAll(tableProperties)
                .build());
    this.schema = schema;
    this.transformer = transformer;
  }

  /**
   * A null transformer means the identity transform.
   *
   * @param record
   * @throws RecordTransformationException
   * @throws DPIcebergRetryableException
   */
  public void add(GenericRecord record)
      throws DPIcebergRetryableException, RecordTransformationException {
    GenericRecord newRecord;
    if (transformer != null) {
      newRecord = transformer.transform(record, schema);
    } else {
      newRecord = record;
    }

    try {
      appendToDataFile(newRecord);
    } catch (DataFileWriter.AppendWriteException e) {
      if (e.getCause() instanceof ClassCastException) {
        throw new RecordTransformationException(e);
      } else {
        throw e;
      }
    } catch (AmazonClientException | AWSClientIOException e) {
      throw new DPIcebergRetryableException("Failed to add record: " + e.getMessage(), e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Builder builder(TableIdentifier tableIdentifier, Table table) {
    return new Builder(tableIdentifier, table);
  }

  public static Builder builder(TableIdentifier tableIdentifier, Table table, Schema tableSchema) {
    return new Builder(tableIdentifier, table, tableSchema);
  }

  /**
   * A builder for the AvroOpenDataFile.
   *
   * <p>A tableId and table must be provided.
   *
   * <p>If the Avro schema for the data file is not provided and a transformer is provided, then
   * this will perform and cache an Avro schema transformation from the current table schema for the
   * first build attempt.
   */
  static class Builder {
    private final Table table;
    private final TableIdentifier tableIdentifier;
    private GenericRecordTransformer transformer;
    private Schema tableSchema;

    private Builder(TableIdentifier tableId, Table table) {
      this(tableId, table, null);
    }

    private Builder(TableIdentifier tableId, Table table, Schema tableSchema) {
      this.table = table;
      this.tableIdentifier = tableId;
      this.transformer = null;
      this.tableSchema = null;
    }

    public Builder withTransformer(GenericRecordTransformer genericRecordTransfomer) {
      this.transformer = genericRecordTransfomer;
      return this;
    }

    private EncryptedOutputFile generateEncryptedOutputFile(StructLike partitionData) {
      OutputFile outputfile =
          table
              .io()
              .newOutputFile(
                  table
                      .locationProvider()
                      .newDataLocation(
                          table.spec(),
                          partitionData,
                          AVRO.addExtension(UUID.randomUUID().toString())));
      return table.encryption().encrypt(outputfile);
    }

    /**
     * @param record an untransformed record from which to construct the table
     * @return an unstarted avro open data file. the record is not added to the table
     * @throws RecordTransformationException this should not be retried with the same record
     * @throws DPIcebergRetryableException this should be retried with the same record
     */
    public AvroOpenDataFile fromRecord(GenericRecord record)
        throws RecordTransformationException, DPIcebergRetryableException {
      Schema dataFileSchema;
      if (transformer != null) {
        if (tableSchema == null) {
          this.tableSchema = AvroSchemaUtil.convert(table.schema(), tableIdentifier.name());
        }
        dataFileSchema = transformer.transformSchema(record, tableSchema);
      } else {
        dataFileSchema = record.getSchema();
      }

      // Because the table can only be partitioned without transformation,
      // it is okay to use the untransformed record to get partitionData
      StructLike partitionData = PartitionHelper.getPartitionData(table.spec(), record);
      EncryptedOutputFile encryptedOutputFile = generateEncryptedOutputFile(partitionData);

      org.apache.iceberg.Schema reassignIdSchema =
          TypeUtil.reassignIds(AvroSchemaUtil.toIceberg(dataFileSchema), table.schema());

      return new AvroOpenDataFile(
          dataFileSchema,
          partitionData,
          table.spec(),
          encryptedOutputFile,
          reassignIdSchema,
          table.properties(),
          transformer);
    }
  }
}
