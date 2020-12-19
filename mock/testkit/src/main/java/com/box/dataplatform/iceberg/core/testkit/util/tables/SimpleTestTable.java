package com.box.dataplatform.iceberg.core.testkit.util.tables;

import com.box.dataplatform.iceberg.core.testkit.MockContextKeyUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;

/** Schema with a single integer field: "myField" */
public class SimpleTestTable {
  public static org.apache.avro.Schema avroSchema =
      SchemaBuilder.record("simple")
          .fields()
          .name("myField")
          .type()
          .intType()
          .noDefault()
          .endRecord();

  public static Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

  public static PartitionSpec partitionSpec = PartitionSpec.builderFor(icebergSchema).build();

  public static GenericRecord createRecord(Integer id) {
    return new GenericRecordBuilder(avroSchema).set("myField", id).build();
  }
}
