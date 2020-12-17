package com.box.dataplatform.iceberg.client.transformations.identity

import com.box.dataplatform.iceberg.client.avro.DPIcebergAvroClient
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.util.IcebergUtils
import com.box.dataplatform.iceberg.core.testkit.util.tables.SimpleTestTable
import com.box.dataplatform.iceberg.{IcebergClientTestBase, IcebergTestUtils}
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.DataFile
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

class SimpleReadWriteTest extends IcebergClientTestBase {

  override def is: SpecStructure =
    s2"""
      DPIcebergClient should be able to read, write and commit time data files
        using the OpenDataFile api ${TestEnv(useOpenDataFileApi = true).work}
        using the Batch api ${TestEnv(useOpenDataFileApi = false).work}
  """

  case class TestEnv(useOpenDataFileApi: Boolean) extends MustThrownMatchers {
    val tableName = DPIcebergTestkit.uniqueTableName("test_avro_table")

    val table = SimpleTestTable.create(tableName)

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
      if (useOpenDataFileApi) {
        writeWithOpenDataFileApi(client, recordBatch)
      } else {
        writeWithBatchApi(client, recordBatch)
      }

    def createBatch(recordsInBatch: Int): List[GenericRecord] =
      (0 until recordsInBatch).map { i =>
        SimpleTestTable.createRecord(i)
      }.toList

    def work = {
      val client = getClient(tableName)

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
