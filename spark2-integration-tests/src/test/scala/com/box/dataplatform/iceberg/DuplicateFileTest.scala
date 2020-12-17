package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit.getOptions
import org.apache.spark.sql.SaveMode
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._

class DuplicateFileTest extends IcebergSparkTestBase with MustThrownMatchers {
  override def is: SpecStructure = s2"""
      SparkSource should handle duplicate file tables
        for normal scans without duplicates ${TestEnv().work}
        for normal scans with duplicates ${TestEnv().normalScan}
        for incremental scans ${TestEnv().incrementalScan}
  """

  case class TestEnv() {
    val ss = spark
    import ss.implicits._
    val tableName = DPIcebergTestkit.uniqueTableName("duplicate")
    val tableId = DPIcebergTestkit.asId(tableName)

    val testRecords = List(TestRecord(0, 0, "0"))
    def writeRecords(): Unit = {
      val ds = spark.createDataset(testRecords)
      val output = ds.write
        .format("dpiceberg")
        .option("write-format", "avro")
        .mode(SaveMode.Append)
        .options(getOptions)
        .save(tableId.toString)
    }

    def normalScan = {
      val table = createTable[TestRecord](tableName, withReadDedupe = true)
      writeRecords()

      val snapshot = table.snapshots().asScala.toList.head
      val file = snapshot.addedFiles().asScala.toList.head
      val appendFiles = table.newFastAppend()
      appendFiles.appendFile(file)
      appendFiles.commit()

      val result = spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(tableId.toString)

      result.as[TestRecord].collect.toList must containTheSameElementsAs(
        testRecords
      )
    }

    def incrementalScan = {
      val testRecords = List(TestRecord(0, 0, "0"))
      val table = createTable[TestRecord](tableName, withReadDedupe = true)
      val ds = spark.createDataset(testRecords)
      writeRecords()
      writeRecords()

      val snapshot0 = table.snapshots().asScala.toList(0)
      val snapshot1 = table.snapshots().asScala.toList(1)
      val file = snapshot1.addedFiles().asScala.toList.head
      val appendFiles = table.newFastAppend()
      appendFiles.appendFile(file)
      appendFiles.commit()

      writeRecords()
      table.refresh()

      val startSnapshotId = snapshot0.snapshotId.toString
      val currentSnapshotId = table.currentSnapshot.snapshotId.toString

      val result = spark.read
        .format("dpiceberg")
        .option(DPIcebergSource.FROM_SNAPSHOT_ID_OPTION, startSnapshotId)
        .option(DPIcebergSource.TO_SNAPSHOT_ID_OPTION, currentSnapshotId)
        .options(getOptions)
        .load(tableId.toString)

      result.as[TestRecord].collect.toList must containTheSameElementsAs(
        testRecords ++ testRecords
      )
    }

    def work = {
      val testRecords = List(TestRecord(0, 0, "0"))
      val table = createTable[TestRecord](tableName, withReadDedupe = true)
      val ds = spark.createDataset(testRecords)

      writeRecords()

      val result = spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(tableId.toString)

      result.as[TestRecord].collect.toList must containTheSameElementsAs(
        testRecords
      )
    }
  }
}
