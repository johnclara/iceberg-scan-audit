package org.apache.iceberg.addons.sanity.spark

import org.apache.iceberg.addons.mock.{MockContext, MockContextId}
import org.apache.iceberg.addons.testkit.MockContextIdUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.specs2.SpecificationWithJUnit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.BeforeAfterAll
import org.specs2.specification.core.SpecStructure

class IcebergSparkTestBase extends SpecificationWithJUnit with MustThrownMatchers with BeforeAfterAll {
  protected var spark: SparkSession = _
  protected var contextId: MockContextId = _

  override def beforeAll(): Unit = {
    contextId = MockContextIdUtil.newContextId();
    val spark_hadoop = "spark.hadoop."
    spark = SparkSession.builder
      .master("local[2]")
      .appName("test")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(SQLConf.PARTITION_OVERWRITE_MODE.key, PartitionOverwriteMode.DYNAMIC.toString.toLowerCase)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    MockContext.clearContext(contextId)
    spark.stop()
  }

  override def is: SpecStructure = s2"""
        IcebergTestBase should
          work
    """
}
