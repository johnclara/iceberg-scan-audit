package org.apache.iceberg.addons.spark.testkit

import scala.reflect.runtime.universe.TypeTag
import org.apache.iceberg.addons.testkit.SampleTableSpec

case class TaggedSampleTableSpec[T <: SampleTableSpec: TypeTag, R <: Product](tableSpec: T)(
    implicit val recordTypeTag: TypeTag[R])
