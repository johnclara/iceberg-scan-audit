package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._

/**
 * Appends TestRecords to a table
 */
object AppendJob {
  import DPIcebergTestkit._
  def run(path: String, spark: SparkSession, withAvro: Boolean): Unit = {
    import spark.implicits._
    val today = 5
    val records = (0 to 500).map(i => TestRecord(i, i % today, String.valueOf((i % today) + 1)))
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
