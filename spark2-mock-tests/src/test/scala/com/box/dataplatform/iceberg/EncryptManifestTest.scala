package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

class EncryptManifestTest extends IcebergSparkTestBase with MustThrownMatchers {
  import DPIcebergTestkit._
  override def is: SpecStructure =
    s2"""
      EncryptManifestTest should
        work with Encrypted and Plain Text Manifest ${TestEnv().work}
  """

  case class TestEnv() {
    val workTableName = uniqueTableName("work")
    val workTableId = asId(workTableName)

    def work = {
      val table = createTable[TestRecord](workTableName)
      val recordsSet1 = (0 to 500).map(i => TestRecord(i, 1, "1")).toList
      append(workTableId.toString, spark, false, recordsSet1)

      val result1 = spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(workTableId.toString)
      val ss = spark
      import ss.implicits._
      result1.as[TestRecord].collect.toList must containTheSameElementsAs(
        recordsSet1,
        (left: TestRecord, right: TestRecord) => left.id == right.id
      )

      table
        .updateProperties()
        .set("dataplatform.manifest.encrypted", "true")
        .set("dataplatform.manifest.encryptionType", "plaintext")
        .commit()
      val recordsSet2 = (0 to 500).map(i => TestRecord(i, 2, "2")).toList
      append(workTableId.toString, spark, false, recordsSet2)
      val result2 = spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(workTableId.toString)
      result2.as[TestRecord].collect.toList must containTheSameElementsAs(
        recordsSet2 ++ recordsSet1,
        (left: TestRecord, right: TestRecord) => left.id == right.id
      )

      table
        .updateProperties()
        .set("dataplatform.manifest.encrypted", "false")
        .set("dataplatform.manifest.encryptionType", "kms")
        .commit()
      val recordsSet3 = (0 to 500).map(i => TestRecord(i, 3, "3")).toList
      append(workTableId.toString, spark, false, recordsSet3)
      val result3 = spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(workTableId.toString)
      result3.as[TestRecord].collect.toList must containTheSameElementsAs(
        recordsSet2 ++ recordsSet1 ++ recordsSet3,
        (left: TestRecord, right: TestRecord) => left.id == right.id
      )
    }

    def append(path: String, spark: SparkSession, withAvro: Boolean, records: List[TestRecord]): Unit = {
      import spark.implicits._
      val today = 5
      val ds = spark.createDataset(records).repartition(10)

      val tableId = TableIdentifier.parse(path)
      val catalogs = DPIcebergTestkit.getCatalog
      val table = catalogs.loadTable(tableId)
      val partitionCols = table.spec().fields().asScala.map(f => col(f.name()))
      val output = ds
        .sortWithinPartitions(partitionCols: _*)
        .write
        .format("dpiceberg")
        .option("write-format", if (withAvro) "avro" else "parquet")
        .mode(SaveMode.Append)
        .options(getOptions)
        .save(path)
    }
  }

}
