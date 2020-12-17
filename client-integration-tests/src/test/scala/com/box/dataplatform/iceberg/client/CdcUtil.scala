package com.box.dataplatform.iceberg.client

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.time.ZonedDateTime

import com.box.dseschema.cdc.credence._
import com.box.dseschema.common.EventMetadata
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecordBase}

import scala.collection.JavaConverters._

case class DPMetadata(version: Long, deleted: Long, operationType: String)

case class CdcTestRecord(pk: String, value: Long, dpmeta: DPMetadata, extraMetadata: Boolean = false)

/**
 * This object provides helper methods when dealing with cdc events.
 */
object CdcUtil {
  private def serializeToBytes[R <: SpecificRecordBase](record: R): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val schema = new Schema.Parser().parse(
      record.getSchema.toString.replace("{\"type\":\"string\",\"avro.java.string\":\"String\"}", "\"string\"")
    )
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    val writer = new SpecificDatumWriter[R](schema)
    writer.write(record, encoder)
    encoder.flush
    out.close
    out.toByteArray
  }

  private def deserializeToGenericRecord[R <: SpecificRecordBase](record: R, bytes: Array[Byte]): GenericRecord = {
    val schema = new Schema.Parser().parse(
      record.getSchema.toString.replace("{\"type\":\"string\",\"avro.java.string\":\"String\"}", "\"string\"")
    )
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null)
    val datum = reader.read(null, decoder)
    datum
  }

  /**
   * This will convert the specific record to a generic record with the utf8 string
   */
  def withUtf8String[R <: SpecificRecordBase](record: R): GenericRecord =
    deserializeToGenericRecord(record, serializeToBytes(record))

  def createRecord(record: CdcTestRecord): CDCEvent = {
    val eventMetadata = new EventMetadata()
    eventMetadata.setEventTimestamp(record.dpmeta.version)
    eventMetadata.setEventId(s"${record.pk}-${record.dpmeta.version}-${record.dpmeta.operationType}")

    val cdcMetadata = new CDCMetadata()
    cdcMetadata.setEntityId(record.pk.toString)
    cdcMetadata.setEntityType("CdcTestRecord")
    cdcMetadata.setVersionEpoch(record.dpmeta.version)
    cdcMetadata.setSource(Source.CREDENCE)

    if (record.extraMetadata) {
      cdcMetadata.setShardId(0)
      val actorMetadata = new ActorMetadata()
      actorMetadata.setAppName("app")
      actorMetadata.setAppVersion("app-version")
      actorMetadata.setRequestId("test-request")
      cdcMetadata.setActor(actorMetadata)
    }

    val firstState = Map[String, Object](
      "value" -> new java.lang.Long(record.value),
      "pk" -> record.pk
    )
    val secondState = Map[String, Object](
      "value" -> new java.lang.Long(record.value - 1),
      "pk" -> record.pk
    )
    val secondDiff = Map[String, Object](
      "value" -> new java.lang.Long(record.value - 1)
    )
    val preSnapshot = {
      val entityState: Option[Map[String, Object]] = record.dpmeta.operationType match {
        case "INSERT" =>
          None
        case "UPDATE" =>
          Some(firstState)
        case "DELETE" =>
          Some(secondState)
      }

      entityState
        .map { entityState =>
          val es = new EntitySnapshot()
          es.setEntityState(entityState.asJava)
          es
        }
        .getOrElse(null)
    }

    val entityDiff = {
      val entityState: Option[Map[String, Object]] = record.dpmeta.operationType match {
        case "INSERT" =>
          Some(firstState)
        case "UPDATE" =>
          Some(secondDiff)
        case "DELETE" =>
          None
      }
      entityState
        .map { entityState =>
          val es = new EntityDiff()
          es.setEntityState(entityState.asJava)
          es
        }
        .getOrElse(null)
    }

    val operation = record.dpmeta.operationType match {
      case "INSERT" => Operation.INSERT
      case "UPDATE" => Operation.UPDATE
      case "DELETE" => Operation.DELETE
    }

    val event = new CDCEvent()
    event.setEventMetadata(eventMetadata)
    event.setCdcMetadata(cdcMetadata)
    event.setOperation(operation)
    event.setPre(preSnapshot)
    event.setDiff(entityDiff)
    event
  }

  def getDPMeta(op: String) = DPMetadata(ZonedDateTime.now().toInstant.toEpochMilli, 0, op)
}
