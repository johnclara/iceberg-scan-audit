package com.box.dataplatform.iceberg.client.avro;

import static org.apache.avro.Schema.Type.*;

import java.util.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

/** utility to update record with correct timestamp. */
public class PartitionTimestampMutator {
  /**
   * update timestamp column in record updating sub partition is not supported.
   *
   * @param table
   * @param record
   * @param timeMultiplier
   */
  public static void mutatePartitionTimestampFields(
      Table table, GenericRecord record, Long timeMultiplier) {
    // if timestamp is compatible return
    if (timeMultiplier == 1) {
      return;
    }

    PartitionSpec partitionSpec = table.spec();
    // no-op for unpartitioned table.
    if (partitionSpec.fields().size() == 0) {
      return;
    }
    PartitionField field = partitionSpec.fields().get(0);

    int lastIndex = field.name().lastIndexOf("_");
    String fieldPath = field.name().substring(0, lastIndex);
    Types.NestedField sourceField = partitionSpec.schema().findField(field.sourceId());
    if (sourceField.type().typeId() == org.apache.iceberg.types.Type.TypeID.TIMESTAMP
        && timeMultiplier > 1) {
      if (fieldPath.contains(".")) {
        String[] parentFields = fieldPath.split("\\.");
        setFieldValue(record, parentFields, 0, timeMultiplier);
        // icebergRecord.set(parentFields[0], record.get(parentFields[0]));
      } else {
        Long r = (Long) record.get(fieldPath);
        record.put(fieldPath, r * timeMultiplier);
      }
    }
  }

  private static void setFieldValue(
      GenericRecord record, String[] parentFields, int i, Long timeMultiplier) {
    if (i >= parentFields.length)
      throw new IllegalStateException(
          String.format(
              "hidden partitioning field: %s not found in record: %s",
              String.join(".", parentFields), record.toString()));
    String pf = parentFields[i];
    if (i == parentFields.length - 1) {
      Long r = (Long) record.get(pf);
      record.put(pf, r * timeMultiplier);
    } else {
      GenericRecord r = (GenericRecord) record.get(pf);
      setFieldValue(r, parentFields, i + 1, timeMultiplier);
    }
  }
}
