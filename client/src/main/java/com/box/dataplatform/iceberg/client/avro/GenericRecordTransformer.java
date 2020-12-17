package com.box.dataplatform.iceberg.client.avro;

import com.box.dataplatform.iceberg.client.exceptions.RecordTransformationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** */
public abstract class GenericRecordTransformer {

  protected abstract GenericRecord unwrappedTransform(
      GenericRecord currentRecord, Schema targetSchema);

  public GenericRecord transform(GenericRecord currentRecord, Schema targetSchema)
      throws RecordTransformationException {
    try {
      return unwrappedTransform(currentRecord, targetSchema);
    } catch (Exception e) {
      throw new RecordTransformationException(e);
    }
  }

  public abstract Schema transformSchema(GenericRecord currentRecord, Schema targetSchema);

  public void logFailedRecord(String tableId, GenericRecord record, Throwable e) {
    // don't log
  }
}
