package org.apache.iceberg.addons.sanity.spark

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.addons.cataloglite.tablespec.TableSpec
import org.apache.iceberg.addons.scanaudit.{ScanAuditEventHandler, ScanAuditTableSchema}
import org.apache.iceberg.addons.spark.testkit.{TaggedSampleTableSpec, Testkit}
import org.apache.iceberg.addons.spark.testkit.sampletables.SimpleRecord
import org.apache.iceberg.addons.testkit.SampleTableSpec
import org.apache.iceberg.addons.testkit.sampletables.SimpleTableSpec
import org.apache.spark.sql.SaveMode
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.core.SpecStructure

object ScanAuditTest {
  case class ScanAuditRecord(
      tableName: String,
      scanId: Long,
      parentPath: Option[String],
      childPath: String,
      evaluatorType: String,
      passedEvaluate: Boolean)

  object ScanAuditTableSpec extends SampleTableSpec {
    import Testkit._
    override protected def namePrefix(): String = "scan-audit"

    override def spec(): TableSpec =
      TableSpec
        .builder(ScanAuditTableSchema.INSTANCE)
        .withPartitionSpec((builder: PartitionSpec.Builder) => builder.build())
        .lockPartitionSpec
        .withProperties(Map.empty[String, String])
        .lockProperties
  }

  implicit def scanAuditTableSpec: TaggedSampleTableSpec[ScanAuditTableSpec.type, ScanAuditRecord] =
    TaggedSampleTableSpec[ScanAuditTableSpec.type, ScanAuditRecord](ScanAuditTableSpec)
}

class ScanAuditTest extends IcebergSparkTestBase with MustThrownMatchers {
  override def is: SpecStructure = s2"""
      ScanAudit should
        work ${TestEnv().work}
  """

  import ScanAuditTest._
  import Testkit._

  case class TestEnv() {
    val ss = spark
    implicit val mck = contextId
    import ss.implicits._

    import scala.collection.JavaConverters._

    def work = {
      val simpleTable = createTable[SimpleTableSpec, SimpleRecord]()

      val records = (0 to 100).map(SimpleRecord(_))
      val simpleDS = records.toDS

      simpleDS
        .writeToTable(simpleTable)
        .mode(SaveMode.Overwrite)
        .save()

      val auditTable = createTable[ScanAuditTableSpec.type, ScanAuditRecord]()
      val listener = new ScanAuditEventHandler(auditTable.table)
      listener.register()

      val readSimpleDS = spark.readTable(simpleTable)

      readSimpleDS.collectAsList().asScala must containTheSameElementsAs(records)

      listener.close()

      spark.readTable(auditTable).show()

      ok
    }
  }
}
