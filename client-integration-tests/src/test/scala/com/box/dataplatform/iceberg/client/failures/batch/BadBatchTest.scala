package com.box.dataplatform.iceberg.client.failures.batch

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.concurrent.TimeUnit

import com.box.dataplatform.iceberg.IcebergClientTestBase
import com.box.dataplatform.iceberg.client.transformations.identity.tables.{FlatTableHelper, NestedTableHelper}
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import org.apache.iceberg.types.Types
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

/**
 *
 */
class BadBatchTest extends IcebergClientTestBase {
  val date1 = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  val date2 = ZonedDateTime.of(2020, 1, 4, 0, 0, 0, 0, ZoneOffset.UTC)

  override def is: SpecStructure =
    s2"""
      DPIcebergClient should throw for
        batches across multiple partitions ${TestEnv().multiplePartition}
        batches with inconsistent schema   ${TestEnv().inconsistentSchema}
        batches with incorrect schema      ${TestEnv().incorrectSchema}
      DPIcebergClient should succeed for
        batches with old schema            ${TestEnv().oldSchema}
  """

  case class TestEnv() extends MustThrownMatchers {
    val tableHelper = FlatTableHelper
    val timeUnit = TimeUnit.MICROSECONDS

    val tableName = DPIcebergTestkit.uniqueTableName("test_avro_table")

    val table = createTable(tableName, tableHelper.icebergSchema, tableHelper.partitionSpec)

    val client = getClient(tableName, timeUnit)

    val goodBatch = (0 until 100).map { i =>
      tableHelper.createRecord("Test" + i, i, date1.toEpochSecond, timeUnit)
    }.toList

    val multiPartitionBatch = (0 until 100).map { i =>
      tableHelper.createRecord("Test" + i, i, date1.toEpochSecond, timeUnit)
    }.toList ++ (100 until 200).map { i =>
      tableHelper.createRecord("Test" + i, i, date2.toEpochSecond, timeUnit)
    }.toList

    val inconsistentSchemaBatch = (0 until 100).map { i =>
      tableHelper.createRecord("Test" + i, i, date1.toEpochSecond, timeUnit)
    }.toList ++ (100 until 200).map { i =>
      NestedTableHelper.createRecord("Test" + i, i, date1.toEpochSecond, timeUnit)
    }.toList

    val incorrectSchemaBatch = (0 until 100).map { i =>
      NestedTableHelper.createRecord("Test" + i, i, date1.toEpochSecond, timeUnit)
    }.toList

    def multiplePartition = {
      val openDataFile = client.createOpenDataFile(multiPartitionBatch.asJava)
      openDataFile.start()
      multiPartitionBatch.foreach(openDataFile.add(_))
      val dataFile = openDataFile.complete()
      client.commitBatch(List(dataFile).asJava)

      /*
       * TODO(jclara) this doesn't actually throw but it should.
       *  We have no guard rails against creating data files not in the same partition
       */

      ok
    }

    def inconsistentSchema = {
      val openDataFile = client.createOpenDataFile(inconsistentSchemaBatch.asJava)
      openDataFile.start()
      inconsistentSchemaBatch.foreach(openDataFile.add(_)) must throwAn[Exception]
      openDataFile.complete()
      ok
    }

    def incorrectSchema =
      client.createOpenDataFile(incorrectSchemaBatch.asJava) must throwAn[Exception]

    def oldSchema = {
      val openDataFile = client.createOpenDataFile(goodBatch.asJava)
      openDataFile.start()
      goodBatch.foreach(openDataFile.add(_))
      val dataFile = openDataFile.complete()
      table.updateSchema().addColumn("newOptionalCol", Types.BooleanType.get()).commit()
      client.commitBatch(List(dataFile).asJava)
      ok
    }
  }
}
