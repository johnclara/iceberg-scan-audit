package com.box.dataplatform.iceberg
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.specs2.SpecificationWithJUnit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.BeforeAfterAll
import org.specs2.specification.core.SpecStructure

import scala.reflect.runtime.universe.TypeTag

class IcebergSparkTestBase extends SpecificationWithJUnit with MustThrownMatchers with BeforeAfterAll {
  protected var spark: SparkSession = _

  def createTable[T <: Product: TypeTag](tableName: String, withReadDedupe: Boolean = false) = {
    val icebergSchema = SparkSchemaUtil.convert(Encoders.product[T].schema)
    val props = Map.empty ++
      (if (withReadDedupe) Some(DPTableProperties.READ_DEDUPE_FILES -> true.toString) else None)

    import scala.collection.JavaConverters._
    DPIcebergTestkit
      .createTable(
        tableName,
        icebergSchema,
        PartitionSpec.builderFor(icebergSchema).build(),
        props.asJava
      )
  }

  override def beforeAll(): Unit = {
    DPIcebergTestkit.start()
    initializeSparkSession()
  }

  def initializeSparkSession(): Unit = {
    val spark_hadoop = "spark.hadoop."
    spark = SparkSession.builder
      .master("local[2]")
      .appName("test")
      .config("spark.driver.bindAddress", "127.0.0.1")
      // DATAPLATFORM SPECIFIC CONFIG START
      .config(SQLConf.PARTITION_OVERWRITE_MODE.key, PartitionOverwriteMode.DYNAMIC.toString.toLowerCase)
      .config("spark.sql.session.timeZone", "UTC")
      // DATAPLATFORM SPECIFIC CONFIG END
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    DPIcebergTestkit.stop()
    spark.stop()
  }

  override def is: SpecStructure = s2"""
        IcebergTestBase should
          work
    """
}
