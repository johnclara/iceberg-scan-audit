package com.box.dataplatform.iceberg

import org.apache.iceberg.{DataFile, DataOperations, Table}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Example of reading iceberg snapshot files to determine what files/partitions were touched
 */
object DataPlatformLog {

  val NoSnapshotId = -1L

  def getSnapshots(table: Table, fromSnapshotId: Long, toSnapshotId: Long): List[Long] = {
    var currentSnapshotId = toSnapshotId
    val endSnapshotId = fromSnapshotId

    val snapshots = mutable.ListBuffer.empty[Long]
    while (currentSnapshotId != endSnapshotId && currentSnapshotId != NoSnapshotId) {
      val snapshot = table.snapshot(currentSnapshotId)
      if (snapshot.operation() != DataOperations.REPLACE) {
        snapshots += currentSnapshotId
      }
      if (snapshot.parentId() == null) {
        currentSnapshotId = NoSnapshotId
      } else {
        currentSnapshotId = snapshot.parentId()
      }
    }

    snapshots.reverse.toList
  }

  def getAffectedValues[F](
      table: Table,
      snapshotId: Long
  )(fileChecker: (DataFile) => F): Set[F] = {
    val snapshot = table.snapshot(snapshotId)

    if (snapshot.operation() != DataOperations.REPLACE) {
      (snapshot.addedFiles().asScala ++ snapshot.deletedFiles().asScala).map(fileChecker(_)).toSet
    } else {
      Set.empty[F]
    }
  }
}
