package com.box.dataplatform.iceberg

import java.util.function.Supplier

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.source.IcebergSource
import org.apache.iceberg.Table
import org.apache.iceberg.addons.mock.{MockCatalog, MockContextId}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, SessionConfigSupport}
import org.slf4j.LoggerFactory

object MockIcebergSource {
  lazy val ContextKeyOption = "iceberg.mock.context.key"
  lazy val ShortName = "mockiceberg"
}

class MockIcebergSource extends IcebergSource with DataSourceRegister {
  private val log = LoggerFactory.getLogger(classOf[MockIcebergSource])

  override def shortName(): String = MockIcebergSource.ShortName

  override def findTable(options: DataSourceOptions, hadoopConf: Configuration): Table = {

    val contextKeyString = options
      .get(MockIcebergSource.ContextKeyOption)
      .orElseThrow(new Supplier[IllegalArgumentException] {
        override def get(): IllegalArgumentException =
          new IllegalArgumentException(s"Missing mock context option: ${MockIcebergSource.ContextKeyOption}")
      });
    val contextKey = new MockContextId(contextKeyString);
    log.info(s"Using mock context $contextKey")

    val catalog = new MockCatalog(contextKey);

    val path = options.get("path").get
    val tableIdentifier = TableIdentifier.parse(path)

    catalog.loadTable(tableIdentifier)
  }
}
