package com.box.dataplatform.iceberg.client.failures.dynamo

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.concurrent.TimeUnit
import java.util.function.Function

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.box.dataplatform.iceberg.IcebergClientTestBase
import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException
import com.box.dataplatform.iceberg.client.transformations.identity.tables.FlatTableHelper
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.dynamo.DynamoTestkit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.AfterEach
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

/**
 *
 */
class BrokenDynamoTests extends IcebergClientTestBase with AfterEach {
  val date = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  var breakableDynamoDB: BreakableDynamoDB = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    DynamoTestkit.wrap(new Function[AmazonDynamoDB, AmazonDynamoDB] {
      override def apply(ddb: AmazonDynamoDB): AmazonDynamoDB = {
        breakableDynamoDB = new BreakableDynamoDB(ddb)
        breakableDynamoDB.mock
      }
    })
  }

  override def is: SpecStructure =
    skipAll ^ sequential ^ s2"""
      DPIcebergClient should throw retryable with broken dynamo
        commiting data file ${TestEnv().committingDataFile}
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

    def committingDataFile = {
      val openDataFile = client.createOpenDataFile(recordBatch.asJava)
      openDataFile.start()
      recordBatch.foreach(openDataFile.add(_))
      val dataFile = openDataFile.complete()
      breakableDynamoDB.startThrowing()
      client.commitBatch(List(dataFile).asJava) must throwAn[DPIcebergRetryableException]
    }
  }

  override protected def after: Any = breakableDynamoDB.fix()
}
