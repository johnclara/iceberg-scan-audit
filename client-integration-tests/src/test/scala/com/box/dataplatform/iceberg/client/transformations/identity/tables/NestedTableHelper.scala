package com.box.dataplatform.iceberg.client.transformations.identity.tables

import java.util.concurrent.TimeUnit

import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.avro.AvroSchemaUtil

/**
 *
 */
object NestedTableHelper extends TimePartitionedTableHelper {
  import TimePartitionedTableHelper._
  val partitionFieldName = "event_metadata.created"

  val microMetadataSchema = SchemaBuilder
    .record("event_metadata")
    .fields()
    .name("created")
    .`type`(getMicroTimestamp)
    .noDefault
    .endRecord()

  val noLogicalTypeSchema = SchemaBuilder
    .record("event_metadata")
    .fields()
    .name("created")
    .`type`(getTimestampWithoutLogicalType)
    .noDefault
    .endRecord()

  val tableSchema: Schema = SchemaBuilder
    .record("event")
    .fields
    .name("name")
    .`type`
    .stringType
    .noDefault
    .name("id")
    .`type`
    .intType
    .noDefault
    .name("event_metadata")
    .`type`(microMetadataSchema)
    .noDefault()
    .endRecord

  val recordSchema: Schema = SchemaBuilder
    .record("event")
    .fields
    .name("id")
    .`type`
    .intType
    .noDefault
    .name("name")
    .`type`
    .stringType
    .noDefault
    .name("event_metadata")
    .`type`(noLogicalTypeSchema)
    .noDefault()
    .endRecord

  val icebergSchema = AvroSchemaUtil.toIceberg(tableSchema)

  val partitionSpec = PartitionSpec
    .builderFor(icebergSchema)
    .day(partitionFieldName)
    .build()

  override def createRecord(name: String, id: Int, created: Long, timeUnit: TimeUnit): GenericRecord = {
    val metadata = new GenericRecordBuilder(noLogicalTypeSchema)
      .set("created", timeUnit.convert(created, TimeUnit.SECONDS))
      .build()

    val record = new GenericRecordBuilder(recordSchema)
      .set("name", s"Test$id")
      .set("id", id)
      .set("event_metadata", metadata)
      .build

    record
  }
}
