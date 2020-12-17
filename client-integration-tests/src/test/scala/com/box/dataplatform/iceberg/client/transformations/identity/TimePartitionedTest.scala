package com.box.dataplatform.iceberg.client.transformations.identity

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.concurrent.TimeUnit

import com.box.dataplatform.iceberg.client.avro.DPIcebergAvroClient
import com.box.dataplatform.iceberg.client.transformations.identity.tables.{
  FlatTableHelper,
  NestedTableHelper,
  TimePartitionedTableHelper
}
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.util.IcebergUtils
import com.box.dataplatform.iceberg.{IcebergClientTestBase, IcebergTestUtils}
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.DataFile
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

class TimePartitionedTest extends IcebergClientTestBase {
  val date1 = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  val date2 = ZonedDateTime.of(2020, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC)
  val date3 = ZonedDateTime.of(2020, 1, 3, 0, 0, 0, 0, ZoneOffset.UTC)
  val date4 = ZonedDateTime.of(2020, 1, 4, 0, 0, 0, 0, ZoneOffset.UTC)

  def toMicro(second: Long) =
    second * 1000000

  override def is: SpecStructure =
    s2"""
      DPIcebergClient should write and commit time partitioned data files
        using the OpenDataFile api
          for NestedTables
            with records written in millis ${TestEnv(
      NestedTableHelper,
      TimeUnit.MILLISECONDS,
      useOpenDataFileApi = true).work}
            with records written in micros ${TestEnv(
      NestedTableHelper,
      TimeUnit.MICROSECONDS,
      useOpenDataFileApi = true).work}
          for FlatTables
            with records written in millis ${TestEnv(FlatTableHelper, TimeUnit.MILLISECONDS, useOpenDataFileApi = true).work}
            with records written in micros ${TestEnv(FlatTableHelper, TimeUnit.MICROSECONDS, useOpenDataFileApi = true).work}
        using the Batch api 
          for NestedTables
            with records written in millis ${TestEnv(
      NestedTableHelper,
      TimeUnit.MILLISECONDS,
      useOpenDataFileApi = false).work}
            with records written in micros ${TestEnv(
      NestedTableHelper,
      TimeUnit.MICROSECONDS,
      useOpenDataFileApi = false).work}
          for FlatTables
            with records written in millis ${TestEnv(FlatTableHelper, TimeUnit.MILLISECONDS, useOpenDataFileApi = false).work}
            with records written in micros ${TestEnv(FlatTableHelper, TimeUnit.MICROSECONDS, useOpenDataFileApi = false).work}
  """

  case class TestEnv(tableHelper: TimePartitionedTableHelper, timeUnit: TimeUnit, useOpenDataFileApi: Boolean)
    extends MustThrownMatchers {
    import IcebergTestUtils._

    val tableName = DPIcebergTestkit.uniqueTableName("test_avro_table")

    val table = createTable(tableName, tableHelper.icebergSchema, tableHelper.partitionSpec)

    /**
     * This will write and commit the batch of records using the open data file api
     */
    def writeWithOpenDataFileApi(client: DPIcebergAvroClient, recordBatch: List[GenericRecord]): DataFile = {
      recordBatch.foreach(client.getPartitionForRecord(_))
      val openDataFile = client.createOpenDataFile(recordBatch.asJava)
      openDataFile.start()
      recordBatch.foreach(openDataFile.add(_))
      val dataFile = openDataFile.complete()
      client.commitBatch(List(dataFile).asJava)
      dataFile
    }

    /**
     * This will write and commit the batch of records using the batch api
     */
    def writeWithBatchApi(client: DPIcebergAvroClient, recordBatch: List[GenericRecord]): DataFile = {
      recordBatch.foreach(client.getPartitionForRecord(_))
      val dataFile = client.processBatch(recordBatch.asJava)
      client.commitBatch(List(dataFile).asJava)
      dataFile
    }

    def write(client: DPIcebergAvroClient, recordBatch: List[GenericRecord]): DataFile =
      if (useOpenDataFileApi) {
        writeWithOpenDataFileApi(client, recordBatch)
      } else {
        writeWithBatchApi(client, recordBatch)
      }

    /**
     * This will make a batch of records where the timestamp in the record is of the given time unit
     */
    def createBatchForDate(recordsInBatch: Int, epochSecond: Long, timeUnit: TimeUnit): List[GenericRecord] =
      (0 until recordsInBatch).map { i =>
        tableHelper.createRecord("Test" + i, i, epochSecond, timeUnit)
      }.toList

    /**
     * This will write three batches of records for day1 and day2.
     * Then it will check the table for files from day1 to day2 and make sure it matches the file written for day1.
     *
     * Then it will write a batch for day3.
     * Then it will check the table for files from day3 to the next day and make sure it matches the file written for day 3.
     */
    def work = {
      val client = getClient(tableName, timeUnit = timeUnit)

      val recordsPerBatch = 100

      val recordBatch1 = createBatchForDate(recordsPerBatch, date1.toEpochSecond, timeUnit)
      val dataFile1 = write(client, recordBatch1)
      dataFile1.recordCount mustEqual recordsPerBatch

      val recordBatch2 = createBatchForDate(recordsPerBatch, date2.toEpochSecond, timeUnit)
      val dataFile2 = write(client, recordBatch2)
      dataFile2.recordCount mustEqual recordsPerBatch

      val dataFilesInRange1 = IcebergUtils
        .getAllDataFilesInRange(
          table,
          toMicro(date1.toEpochSecond),
          toMicro(date2.toEpochSecond),
          tableHelper.partitionFieldName
        )
        .asScala
        .toList
      checkFilesEqual(dataFilesInRange1, List(dataFile1))

      val recordBatch3 = createBatchForDate(recordsPerBatch, date3.toEpochSecond, timeUnit)
      val dataFile3 = write(client, recordBatch3)
      dataFile3.recordCount mustEqual recordsPerBatch

      val dataFilesInRange2 = IcebergUtils
        .getAllDataFilesInRange(
          table,
          toMicro(date3.toEpochSecond),
          toMicro(date3.toEpochSecond) + TimeUnit.DAYS.toMicros(1),
          tableHelper.partitionFieldName
        )
        .asScala
        .toList

      checkFilesEqual(dataFilesInRange2, List(dataFile3))
    }
  }

}
