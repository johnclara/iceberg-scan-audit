package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit
import org.specs2.SpecificationWithJUnit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.specification.BeforeAfterAll

abstract class ControlPlaneTestBase extends SpecificationWithJUnit with MustThrownMatchers with BeforeAfterAll {

  override def beforeAll(): Unit =
    DPIcebergTestkit.start()

  override def afterAll(): Unit =
    DPIcebergTestkit.stop()
}
