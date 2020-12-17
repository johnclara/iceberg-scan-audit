package com.box.dataplatform.iceberg.client.transformations.entity

import java.{lang, util}

import com.box.dataplatform.iceberg.client.CdcUtil
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.util.IcebergUtils
import com.box.dataplatform.iceberg.IcebergClientTestBase
import com.box.dataplatform.{DpMetadata, TestRecordWithMetadata}
import com.box.dseschema.cdc.credence._
import com.box.dseschema.common.EventMetadata
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg._
import org.apache.iceberg.avro.AvroSchemaUtil
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

/**
 * client test using internal API.
 */
class EntityTransformTest extends IcebergClientTestBase with MustThrownMatchers {

  override def is: SpecStructure =
    s2"""
      EntityTransformTest should
        write avro files and read avro files ${TestEnv().work}
  """
  case class TestEnv() {
    val tableName = DPIcebergTestkit.uniqueTableName("entity_transform")
    val recordSchema = new CDCEvent().getSchema
    val eventSchema = new TestRecordWithMetadata().getSchema

    val table: Table = {
      val icebergSchema = AvroSchemaUtil.toIceberg(eventSchema)
      val partitionSpec = PartitionSpec
        .builderFor(icebergSchema)
        .build()
      createTable(tableName, icebergSchema, partitionSpec, withEntityTransformation = true)
    }

    def work = {
      val insertTime1 = 100L
      val insertTime2 = 200L

      // cdc events
      val event1 = getCdcEvent(insertTime1, extraMetadata = false)
      val event2 = getCdcEvent(insertTime2, extraMetadata = true)
      val badEvent = getCdcEvent(insertTime1, extraMetadata = true, isBad = true) // fails entity transformation
      val cdcEvents = List(event1, event2, badEvent).map(CdcUtil.withUtf8String(_))

      val testRecordSchema = new TestRecordWithMetadata().getSchema

      val expectedEntity1 = new TestRecordWithMetadata()
      expectedEntity1.setTimestamp(insertTime1)
      expectedEntity1.setValue(1)

      val dpMetadata1 = new DpMetadata()
      dpMetadata1.setDeleted(0)
      dpMetadata1.setOperationType("INSERT")
      dpMetadata1.setVersion(0)
      dpMetadata1.setEventTime(insertTime1)
      expectedEntity1.setDpmeta(dpMetadata1)

      val expectedEntity2 = new TestRecordWithMetadata()
      expectedEntity2.setTimestamp(insertTime2)
      expectedEntity2.setValue(1)
      val dpMetadata2 = new DpMetadata()
      dpMetadata2.setDeleted(0)
      dpMetadata2.setOperationType("INSERT")
      dpMetadata2.setVersion(0)
      dpMetadata2.setEventTime(insertTime2)
      dpMetadata2.setShardId("0")
      dpMetadata2.setRequestId("test-request")
      expectedEntity2.setDpmeta(dpMetadata2)

      // test record events
      val expectedRecords = List[GenericRecord](expectedEntity1, expectedEntity2)

      val client = getClient(tableName)
      val dataFile = client.processBatch(cdcEvents.asJava)
      client.commitBatch(util.Arrays.asList(dataFile))

      val dataFiles = IcebergUtils.getAllDataFiles(table)
      val records = IcebergUtils.getAllGenericRecords(table, dataFiles).asScala

      records.map(_.toString) must containTheSameElementsAs(expectedRecords.map(_.toString))
      records.size mustEqual 2
    }
  }

  private def getCdcEvent(insertTime: Long, extraMetadata: Boolean, isBad: Boolean = false): CDCEvent = {
    val eventMetadata = new EventMetadata
    eventMetadata.setEventTimestamp(insertTime)
    eventMetadata.setEventId(insertTime.toString + ".insert")

    val cdcMetadata = new CDCMetadata
    cdcMetadata.setEntityId(insertTime.toString)
    cdcMetadata.setEntityType("TestRecord")
    cdcMetadata.setVersionEpoch(0)
    cdcMetadata.setSource(Source.CREDENCE)

    if (extraMetadata) {
      cdcMetadata.setShardId(0)
      val actorMetadata = new ActorMetadata()
      actorMetadata.setAppName("app")
      actorMetadata.setAppVersion("app-version")
      actorMetadata.setRequestId("test-request")
      cdcMetadata.setActor(actorMetadata)
    }

    val entityDiff = new EntityDiff
    val entityState =
      if (!isBad)
        Map[String, Object](
          "value" -> new lang.Integer(1),
          "timestamp" -> new lang.Long(insertTime).toString
        ).asJava
      else
        Map[String, Object](
          "value" -> new java.lang.String("0"),
          "timestamp" -> new lang.Long(insertTime)
        ).asJava
    entityDiff.setEntityState(entityState)

    val event = new CDCEvent
    event.setEventMetadata(eventMetadata)
    event.setCdcMetadata(cdcMetadata)
    event.setOperation(Operation.INSERT)
    event.setDiff(entityDiff)
    event
  }
}
