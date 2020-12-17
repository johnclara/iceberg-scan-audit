package com.box.dataplatform.iceberg
import java.util.concurrent.TimeUnit

import com.box.dataplatform.iceberg.client.avro.DPIcebergAvroClient
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.s3.S3Testkit
import org.specs2.SpecificationWithJUnit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.BeforeAfterAll
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.{PartitionSpec, Table, Schema => IcebergSchema}

import scala.collection.JavaConverters._

abstract class IcebergClientTestBase extends SpecificationWithJUnit with MustThrownMatchers with BeforeAfterAll {
  import DPIcebergTestkit._

  def getClient(
      tableName: String,
      timeUnit: TimeUnit = TimeUnit.MICROSECONDS,
      useFastAppend: Boolean = true): DPIcebergAvroClient =
    new DPIcebergAvroClient(
      TENANT_NAME,
      tableName,
      getCatalog,
      timeUnit,
      useFastAppend
    )

  def createTable(
      tableName: String,
      schema: IcebergSchema,
      partitionSpec: PartitionSpec,
      withManifestEncryption: Boolean = true,
      withEntityTransformation: Boolean = false): Table = {
    val tableIdentifier = TableIdentifier.of(TENANT_NAME, tableName)
    val catalog = getCatalog
    val tableLocation = s"s3a://${S3Testkit.S3_BUCKET}/${DPIcebergTestkit.TENANT_NAME}.db/$tableName"

    // we only need to set this during test to avoid error reported for metadata directory
    val tbProps = Map(
      //TableProperties.WRITE_METADATA_LOCATION -> s"$tableLocation/meta_test"
    ) ++ (
      if (withManifestEncryption) {
        Map(DPTableProperties.MANIFEST_ENCRYPTED -> "true")
      } else {
        Map.empty[String, String]
      }
    ) ++ (
      if (withEntityTransformation) {
        Map(DPTableProperties.WRITE_WITH_ENTITY_TRANSFORM -> "true")
      } else {
        Map.empty[String, String]
      }
    )

    val table = catalog.createTable(
      tableIdentifier,
      schema,
      partitionSpec,
      tableLocation,
      tbProps.asJava
    )
    val loadedTable = catalog.loadTable(tableIdentifier)
    table
  }

  override def beforeAll(): Unit =
    DPIcebergTestkit.start()

  override def afterAll(): Unit =
    DPIcebergTestkit.stop()
}
