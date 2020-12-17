package com.box.dataplatform.iceberg

import com.box.dataplatform.iceberg.core.testkit.util.IcebergUtils
import org.apache.iceberg.DataFile
import org.specs2.matcher.MustThrownMatchers

object IcebergTestUtils extends MustThrownMatchers {
  def checkFilesEqual(filesA: List[DataFile], filesB: List[DataFile]) =
    filesA.map(f => (f.path().toString, IcebergUtils.toString(f.partition()))) must containTheSameElementsAs(
      filesB.map(f => (f.path().toString, IcebergUtils.toString(f.partition))))
}
