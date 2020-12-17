package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.DPCatalog
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.source.IcebergSource
import org.apache.iceberg.{DPBaseTable, Table}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.slf4j.LoggerFactory

object DPIcebergSource {
  val FROM_SNAPSHOT_ID_OPTION = "dpiceberg.snapshot.from"
  val TO_SNAPSHOT_ID_OPTION = "dpiceberg.snapshot.to"
}

class DPIcebergSource extends IcebergSource with DataSourceRegister {
  private val log = LoggerFactory.getLogger(classOf[DPIcebergSource])

  override def shortName(): String = "dpiceberg"

  override def findTable(options: DataSourceOptions, hadoopConf: Configuration): Table = {
    //FROM_SNAPSHOT_ID_OPTION exclusive and up to TO_SNAPSHOT_ID_OPTION inclusive
    val fromSnapshotId = Option(options.get(DPIcebergSource.FROM_SNAPSHOT_ID_OPTION).orElse(null)).map(_.toLong)
    val toSnapshotId = Option(options.get(DPIcebergSource.TO_SNAPSHOT_ID_OPTION).orElse(null)).map(_.toLong)

    val optionsMap = options.asMap();
    log.info(s"Using options $optionsMap")

    val catalog = DPCatalog.load(optionsMap, hadoopConf);

    val path = options.get("path").get
    val tableIdentifier = TableIdentifier.parse(path)

    val table = catalog.loadTable(tableIdentifier)

    // TODO get rid of this weird injection
    table match {
      case baseTable: DPBaseTable =>
        fromSnapshotId.foreach(baseTable.withFromSnapshotId(_))
        toSnapshotId.foreach(baseTable.withToSnapshotId(_))
      case _ =>
    }
    table
  }
}
