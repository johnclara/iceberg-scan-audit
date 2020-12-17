package com.box.dataplatform.iceberg.client.transformations.identity.tables

import java.util
import java.util.Map

import com.box.dataplatform.iceberg.IcebergClientTestBase
import com.box.dataplatform.iceberg.client.avro.DPIcebergAvroClient
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.s3.S3Testkit
import com.box.dataplatform.iceberg.core.testkit.util.IcebergUtils
import com.box.dataplatform.iceberg.core.testkit.util.tables.SimpleTestTable
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.{DataFile, PartitionSpec, Schema, Table}
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

class ReadWriteWithConfiguredRootTest extends IcebergClientTestBase {

  override def is: SpecStructure =
    s2"""
      DPIcebergClient should be able to read, write and commit time data files
        using the OpenDataFile api ${TestEnv(useOpenDataFileApi = true).work}
  """

  case class TestEnv(useOpenDataFileApi: Boolean) extends MustThrownMatchers {
    val tableName = DPIcebergTestkit.uniqueTableName("test_avro_table")

    val table = SimpleTestTable.create(tableName)

    DPIcebergTestkit.getCatalog
      .createTable(
        DPIcebergTestkit.asId(tableName),
        SimpleTestTable.icebergSchema,
        SimpleTestTable.partitionSpec,
        Map(
          "read.spark.disable-estimate-statistics" -> "true",
          "write.avro.compression-codec" -> "snappy",
          "write.metadata.path" -> s"s3a://${S3Testkit.S3_BUCKET}/${tableName}/metadata",
          "dataplatform.write.with-entity-transform" -> "true",
          "write.parquet.compression-codec" -> "snappy",
          "write.folder-storage.path" -> s"s3a://${S3Testkit.S3_BUCKET}/${tableName}/data",
          "dataplatform.read.dedupe.files" -> "true",
          "write.object-storage.enabled" -> "false"
        ).asJava
      )

    def writeWithOpenDataFileApi(client: DPIcebergAvroClient, recordBatch: List[GenericRecord]): DataFile = {
      recordBatch.foreach(client.getPartitionForRecord(_))
      val openDataFile = client.createOpenDataFile(recordBatch.asJava)
      openDataFile.start()
      recordBatch.foreach(openDataFile.add(_))
      val dataFile = openDataFile.complete()
      client.commitBatch(List(dataFile).asJava)
      dataFile
    }

    def writeWithBatchApi(client: DPIcebergAvroClient, recordBatch: List[GenericRecord]): DataFile = {
      recordBatch.foreach(client.getPartitionForRecord(_))
      val dataFile = client.processBatch(recordBatch.asJava)
      client.commitBatch(List(dataFile).asJava)
      dataFile
    }

    def write(client: DPIcebergAvroClient, recordBatch: List[GenericRecord]): DataFile =
      if (useOpenDataFileApi) writeWithOpenDataFileApi(client, recordBatch) else writeWithBatchApi(client, recordBatch)

    def createBatch(recordsInBatch: Int): List[GenericRecord] =
      (0 until recordsInBatch).map { i =>
        SimpleTestTable.createRecord(i)
      }.toList

    def work = {
      val client = getClient(tableName, useFastAppend = true)

      val recordsPerBatch = 100

      val recordBatch1 = createBatch(recordsPerBatch)
      val dataFile1 = write(client, recordBatch1)
      dataFile1.recordCount mustEqual recordsPerBatch

      val recordBatch2 = createBatch(recordsPerBatch)
      val dataFile2 = write(client, recordBatch2)
      dataFile2.recordCount mustEqual recordsPerBatch

      IcebergUtils.getAllGenericRecords(table).size mustEqual recordsPerBatch * 2
    }
  }

}
