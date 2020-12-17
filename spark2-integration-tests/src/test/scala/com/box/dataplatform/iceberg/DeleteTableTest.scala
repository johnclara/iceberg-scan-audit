package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import org.apache.iceberg.exceptions.NoSuchTableException
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

class DeleteTableTest extends IcebergSparkTestBase with MustThrownMatchers {
  import DPIcebergTestkit._
  override def is: SpecStructure = s2"""
      DeleteTableTest should
        delete table ${TestEnv().testDeleteTable}
  """

  case class TestEnv() {
    def testDeleteTable = {
      val testTable = uniqueTableName("deletetest")
      val testTableId = DPIcebergTestkit.asId(testTable)
      val table = createTable[TestRecord](testTable)
      AppendJob.run(testTableId.toString, spark, false)

      spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(testTableId.toString + ".history")
        .count() mustEqual (1)

      val result = spark.read
        .format("dpiceberg")
        .options(getOptions)
        .load(testTableId.toString)
      val ss = spark
      result.show(10)
      result.count() mustEqual (501)

      val catalog = DPIcebergTestkit.getCatalog
      val deleted = catalog.dropTable(testTableId, true)
      deleted mustEqual (true)

      catalog.loadTable(testTableId) must throwA[NoSuchTableException]
    }
  }
}
