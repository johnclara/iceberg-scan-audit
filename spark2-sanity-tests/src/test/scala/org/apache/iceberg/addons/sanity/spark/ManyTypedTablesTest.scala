package org.apache.iceberg.addons.sanity.spark

import org.apache.iceberg.addons.cataloglite.tablespec.TableSpec
import org.apache.iceberg.addons.spark.testkit.sampletables.SimpleRecord
import org.apache.iceberg.addons.spark.testkit.{TaggedSampleTableSpec, Testkit}
import org.apache.iceberg.addons.testkit.SampleTableSpec
import org.apache.iceberg.addons.testkit.sampletables.SimpleTableSpec
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField.optional
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.spark.sql.SaveMode
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

object MoreTableSpecs {
  case class StringValueRecord(myValue: String)
  object StringValueTableSpec extends SampleTableSpec {
    import Testkit._
    override protected def namePrefix(): String = "manytest"

    override def spec(): TableSpec =
      TableSpec
        .builder(new Schema(optional(0, "myValue", Types.StringType.get)))
        .withPartitionSpec((builder: PartitionSpec.Builder) => builder.build())
        .lockPartitionSpec
        .withProperties(Map.empty[String, String])
        .lockProperties
  }

  implicit def stringValueTableSpec: TaggedSampleTableSpec[StringValueTableSpec.type, StringValueRecord] =
    TaggedSampleTableSpec[StringValueTableSpec.type, StringValueRecord](StringValueTableSpec)
}

class ManyTypedTablesTest extends IcebergSparkTestBase with MustThrownMatchers {
  override def is: SpecStructure = s2"""
      ManyTypedTablesTest should
        read and write with more than one TaggedTableSpec in scope ${TestEnv().readWrite}
  """

  import MoreTableSpecs._
  import Testkit._

  case class TestEnv() {
    val ss = spark
    implicit val mck = contextId
    import ss.implicits._

    import scala.collection.JavaConverters._

    def readWrite = {
      val oldTable = createTable[SimpleTableSpec, SimpleRecord]()
      val records = (0 to 100).map(SimpleRecord(_))
      val oldDS = records.toDS

      oldDS
        .writeToTable(oldTable)
        .mode(SaveMode.Overwrite)
        .save()

      val readDs = spark.readTable(oldTable)

      readDs.collectAsList().asScala must containTheSameElementsAs(records)

      val oldTable1 = createTable[StringValueTableSpec.type, StringValueRecord]()
      val records1 = (0 to 100).map(i => StringValueRecord(i.toString))
      val oldDS1 = records1.toDS

      oldDS1
        .writeToTable(oldTable1)
        .mode(SaveMode.Overwrite)
        .save()

      val readDs1 = spark.readTable(oldTable1)

      readDs1.collectAsList().asScala must containTheSameElementsAs(records1)
    }
  }
}
