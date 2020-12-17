package com.box.dataplatform.iceberg.client.transformations.identity.tables
import java.util.concurrent.TimeUnit

import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.avro.AvroSchemaUtil

/**
 *
 */
object FlatTableHelper extends TimePartitionedTableHelper {
  import TimePartitionedTableHelper._
  val partitionFieldName = "created"

  val tableSchema: Schema =
    SchemaBuilder
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
      .name("created")
      .`type`(getMicroTimestamp)
      .noDefault()
      .endRecord

  val recordSchema: Schema =
    SchemaBuilder
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
      .name("created")
      .`type`(getTimestampWithoutLogicalType)
      .noDefault()
      .endRecord

  val icebergSchema = AvroSchemaUtil.toIceberg(tableSchema)

  val partitionSpec = PartitionSpec
    .builderFor(icebergSchema)
    .day(partitionFieldName)
    .build()

  def createRecord(name: String, id: Int, epochSecondCreated: Long, timeUnit: TimeUnit): GenericRecord = {
    val record = new GenericRecordBuilder(tableSchema)
      .set("name", s"Test$id")
      .set("id", id)
      .set("created", timeUnit.convert(epochSecondCreated, TimeUnit.SECONDS))
      .build

    record
  }
}
