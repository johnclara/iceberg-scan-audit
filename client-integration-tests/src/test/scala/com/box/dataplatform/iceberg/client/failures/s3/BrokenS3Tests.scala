package com.box.dataplatform.iceberg.client.failures.s3

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.concurrent.TimeUnit

import com.box.dataplatform.iceberg.IcebergClientTestBase
import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException
import com.box.dataplatform.iceberg.client.transformations.identity.tables.FlatTableHelper
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.AfterEach
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

class BrokenS3Tests extends IcebergClientTestBase with AfterEach {
  val date = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

  override def beforeAll(): Unit =
    super.beforeAll()

  // TODO reenable the broken s3 tests
  override def is: SpecStructure =
    skipAll ^ s2"""
      DPIcebergClient should throw retryable for broken s3
        when starting open data file ${TestEnv().startingOpenDataFile}
        when closing data file ${TestEnv().closingOpenDataFile}
        when committing data file ${TestEnv().committingDataFile}
  """

  case class TestEnv() extends MustThrownMatchers {
    val tableHelper = FlatTableHelper
    val timeUnit = TimeUnit.MICROSECONDS

    val tableName = DPIcebergTestkit.uniqueTableName("test_avro_table")

    val table = createTable(tableName, tableHelper.icebergSchema, tableHelper.partitionSpec)

    val client = getClient(tableName, timeUnit)

    val recordBatch = (0 until 100).map { i =>
      tableHelper.createRecord("Test" + i, i, date.toEpochSecond, timeUnit)
    }.toList

    def startingOpenDataFile = {
      BreakableS3Factory.startThrowing()
      val openDataFile = client.createOpenDataFile(recordBatch.asJava)
      openDataFile.start() must throwAn[DPIcebergRetryableException]
    }

    def closingOpenDataFile = {
      val openDataFile = client.createOpenDataFile(recordBatch.asJava)
      openDataFile.start()
      openDataFile.add(recordBatch.head)
      BreakableS3Factory.startThrowing()
      openDataFile.complete() must throwAn[DPIcebergRetryableException]
    }

    def committingDataFile = {
      val openDataFile = client.createOpenDataFile(recordBatch.asJava)
      openDataFile.start()
      recordBatch.foreach(openDataFile.add(_))
      val dataFile = openDataFile.complete()
      BreakableS3Factory.startThrowing()
      client.commitBatch(List(dataFile).asJava) must throwAn[DPIcebergRetryableException]
    }
  }

  override protected def after: Any =
    BreakableS3Factory.fix()
}
