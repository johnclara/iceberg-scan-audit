package org.apache.iceberg;

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PartitionHelper {
  public static PartitionData getPartitionData(PartitionSpec spec, GenericRecord record) {
    PartitionData partition = new PartitionData(spec.partitionType());

    for (int i = 0; i < spec.fields().size(); i++) {
      PartitionField field = spec.fields().get(0);
      Transform<Object, Object> transform = (Transform<Object, Object>) field.transform();
      Types.NestedField sourceField = spec.schema().findField(field.sourceId());
      if (sourceField.type().typeId() == Type.TypeID.TIMESTAMP) {
        // iceberg adds _day, _hour to all hidden partition field.
        // to get name of source partition use hidden partition field minus transformation named
        // added by iceberg.
        int lastIndex = field.name().lastIndexOf("_");
        String[] fieldPath = field.name().substring(0, lastIndex).split("\\.");
        partition.set(i, transform.apply(getFieldInAvroRecord(fieldPath, record)));
      } else {
        String[] fieldPath = field.name().split("\\.");
        partition.set(i, transform.apply(getFieldInAvroRecord(fieldPath, record)));
      }
    }

    return partition;
  }

  private static Object getFieldInAvroRecord(String[] fieldPath, GenericRecord record) {
    GenericRecord curr = record;
    for (int i = 0; i < fieldPath.length - 1; i++) {
      String field = fieldPath[i];
      curr = (GenericRecord) curr.get(field);
    }
    String lastField = fieldPath[fieldPath.length - 1];
    return curr.get(lastField);
  }
}
