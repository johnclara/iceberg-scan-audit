package com.box.dataplatform.iceberg.client.transformations.identity.tables

import java.util.concurrent.TimeUnit

import org.apache.avro.generic.GenericRecord
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.iceberg.{PartitionSpec, Schema => IcebergSchema}

object TimePartitionedTableHelper {
  def getMilliTimestamp: Schema = {
    val timeSchema = Schema.create(Schema.Type.LONG)
    timeSchema.addProp("adjust-to-utc", false)
    val timestampMilliType: Schema = LogicalTypes.timestampMillis.addToSchema(timeSchema)
    timestampMilliType
  }

  def getMicroTimestamp: Schema = {
    val timeSchema = Schema.create(Schema.Type.LONG)
    timeSchema.addProp("adjust-to-utc", false)
    val timestampMilliType: Schema = LogicalTypes.timestampMicros().addToSchema(timeSchema)
    timestampMilliType
  }

  def getTimestampWithoutLogicalType: Schema =
    Schema.create(Schema.Type.LONG)
}

trait TimePartitionedTableHelper extends TableHelper {
  def partitionFieldName: String
  def createRecord(name: String, id: Int, created: Long = 0L, timeUnit: TimeUnit = TimeUnit.MICROSECONDS): GenericRecord
}

trait TableHelper {
  def icebergSchema: IcebergSchema
  def partitionSpec: PartitionSpec
}
