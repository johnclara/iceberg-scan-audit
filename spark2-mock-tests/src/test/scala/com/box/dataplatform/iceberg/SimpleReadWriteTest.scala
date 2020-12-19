package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import com.box.dataplatform.iceberg.core.testkit.util.tables.SimpleTestTable
import org.apache.spark.sql.{Dataset, SaveMode}
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

class SimpleReadWriteTest extends IcebergSparkTestBase with MustThrownMatchers {
  import DPIcebergTestkit._
  override def is: SpecStructure = s2"""
      SimpleReadWriteTest should
        read and write ${TestEnv().readWrite}
  """

  case class TestEnv() {
    val ss = spark
    import ss.implicits._
    import scala.collection.JavaConverters._
    def readWrite = {
      val tableName = uniqueTableName("readwrite")
      val tableId = asId(tableName)
      val table = SimpleTestTable.create(tableName)

      val records = (0 to 100).map(SimpleTestRecord(_))
      val ogDf: Dataset[SimpleTestRecord] = records.toDS

      val options = getOptions

      ogDf.write
        .format("dpiceberg")
        .option("write-format", "avro")
        .mode(SaveMode.Overwrite)
        .options(getOptions)
        .save(tableId.toString)

      val readDf: Dataset[SimpleTestRecord] = spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(tableId.toString)
        .as[SimpleTestRecord]

      readDf.collectAsList().asScala must containTheSameElementsAs(records)
    }
  }
}
