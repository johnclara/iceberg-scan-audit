package org.apache.iceberg.addons.sanity.spark

import org.apache.iceberg.addons.spark.testkit.Testkit
import org.apache.iceberg.addons.spark.testkit.sampletables.SimpleRecord
import org.apache.iceberg.addons.testkit.sampletables.SimpleTableSpec
import org.apache.spark.sql.SaveMode
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

class SimpleReadWriteTest extends IcebergSparkTestBase with MustThrownMatchers {
  override def is: SpecStructure = s2"""
      SimpleReadWriteTest should
        read and write ${TestEnv().readWrite}
  """

  import Testkit._

  import scala.collection.JavaConverters._

  case class TestEnv() {
    val ss = spark
    implicit val mck = contextId
    import ss.implicits._

    def readWrite = {
      val table = createTable[SimpleTableSpec, SimpleRecord]()
      val records = (0 to 100).map(SimpleRecord(_))
      val ogDs = records.toDS

      ogDs
        .writeToTable(table)
        .mode(SaveMode.Overwrite)
        .save()

      val readDs = spark.readTable(table)

      readDs.collectAsList().asScala must containTheSameElementsAs(records)
    }
  }
}
