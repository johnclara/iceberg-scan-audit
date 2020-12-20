package org.apache.iceberg.addons.spark.testkit

import com.box.dataplatform.iceberg.MockIcebergSource
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.addons.cataloglite.TableSpec
import org.apache.iceberg.addons.mock.MockContextId
import org.apache.iceberg.addons.spark.testkit.sampletables.SimpleRecord
import org.apache.iceberg.addons.testkit.sampletables.SimpleTableSpec
import org.apache.iceberg.addons.testkit.{SampleTableSpec, TableIdentifierUtil}
import org.apache.iceberg.catalog.TableIdentifier
import java.util.function.{Function => JavaFunction}

import scala.reflect.runtime.universe.TypeTag
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.{DataFrameWriter, Dataset, Encoder, SparkSession}

/**
 * Scala/spark specific builder helpers
 */
object Testkit {
  def newTableId(prefix: String): TableIdentifier = TableIdentifierUtil.newTableId(prefix)
  def newTableId: TableIdentifier = TableIdentifierUtil.newTableId()

  implicit class TableBuilderSupport(builder: TableSpec) {
    def withProperties(properties: Map[String, String]): TableSpec = {
      import scala.collection.JavaConverters._
      builder.withProperties(properties.asJava)
    }
    def withPartitionSpec(fromBuilder: Function[PartitionSpec.Builder, PartitionSpec]): TableSpec =
      builder.withPartitionSpec(new JavaFunction[PartitionSpec.Builder, PartitionSpec] {
        override def apply(t: PartitionSpec.Builder): PartitionSpec = fromBuilder.apply(t)
      })
  }

  implicit class SampleTableSupport(sampleTable: SampleTableSpec) {
    def create[T <: Product](implicit e: Encoder[T]) = {
      val icebergSchema = SparkSchemaUtil.convert(e.schema)
      TableSpec.builder(icebergSchema)
    }
  }

  implicit class SparkSessionSupport(spark: SparkSession) {
    def readTable[T <: SampleTableSpec, R <: Product](table: SampleTable[T, R]): Dataset[R] = {
      import spark.implicits._
      implicit val recordTypeTag = table.taggedSpec.recordTypeTag
      spark.read
        .format(MockIcebergSource.ShortName)
        .option(MockIcebergSource.ContextKeyOption, table.contextId.mockContext())
        .load(table.tableId.toString)
        .as[R]
    }
  }

  implicit class DatasetSupport[R <: Product](ds: Dataset[R]) {
    def writeToTable[T <: SampleTableSpec](table: SampleTable[T, R]): DataFrameWriter[R] =
      ds.write
        .format(MockIcebergSource.ShortName)
        .option(MockIcebergSource.ContextKeyOption, table.contextId.mockContext())
        .option("path", table.tableId.toString)
  }

  implicit def simpleTableSpec: TaggedSampleTableSpec[SimpleTableSpec, SimpleRecord] =
    TaggedSampleTableSpec[SimpleTableSpec, SimpleRecord](SimpleTableSpec.INSTANCE)

  def createTable[T <: SampleTableSpec: TypeTag, R <: Product: TypeTag]()(
      implicit spec: TaggedSampleTableSpec[T, R],
      context: MockContextId): SampleTable[T, R] = {
    assert(context != null, "a null context was provided")
    val tableId = spec.tableSpec.newId()
    val table = spec.tableSpec.create(tableId, context)
    SampleTable[T, R](tableId, table, spec, context)
  }
}
