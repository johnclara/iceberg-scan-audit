package org.apache.iceberg.addons.spark.testkit

import org.apache.iceberg.Table
import org.apache.iceberg.addons.mock.MockContextId

import scala.reflect.runtime.universe.TypeTag
import org.apache.iceberg.addons.testkit.SampleTableSpec
import org.apache.iceberg.catalog.TableIdentifier

case class SampleTable[T <: SampleTableSpec: TypeTag, R <: Product](
    tableId: TableIdentifier,
    table: Table,
    implicit val taggedSpec: TaggedSampleTableSpec[T, R],
    implicit val contextId: MockContextId)
