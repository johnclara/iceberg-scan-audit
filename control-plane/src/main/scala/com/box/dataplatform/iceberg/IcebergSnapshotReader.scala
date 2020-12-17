package com.box.dataplatform.iceberg

import java.time.{Clock, Instant}
import java.util.NoSuchElementException

import org.apache.iceberg.{DPManifestUtils, DataFile, PartitionSpec, Snapshot, StructLike, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.types.Conversions
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

case class SnapshotTimestamp(snapshotId: Long, timestamp: Instant)

case class AffectedPartitions(
    partitions: Set[Partition],
    upToSnapshot: Long,
    isUptToDate: Boolean,
    snapshotTime: Instant)

object IcebergSnapshotReader {
  val NoSnapshotId = -1L
  val NoSnapshot = SnapshotTimestamp(NoSnapshotId, Instant.EPOCH)
}

trait IcebergSnapshotReader {

  /**
   * Get affected partitions of a table from the last seen snapshot to the current snapshot. Assumes all manifests are using the current partition
   * since we don't currently allow partition changes.
   * @param tableId the id of the table
   * @param lastSeenSnapshotId the last snapshot seen by the client. Will return all partitions touched after this snapshot
   * @throws IllegalArgumentException if toSnapshotId is chronologically older than fromSnapshotId
   * @throws NoSuchElementException if a snapshot id is not found in the history table
   * @return A set of partitions representing the iceberg partitions that were modified
   */
  def getAffectedPartitions(tableId: TableIdentifier, lastSeenSnapshotId: Long): AffectedPartitions

  def getAffectedPrimaryPartitions(
      tableId: TableIdentifier,
      lastSeenSnapshotId: Long,
      windowStart: Int,
      windowEnd: Int): AffectedPartitions

  def getCurrentSnapshot(tableId: TableIdentifier): SnapshotTimestamp

  def getSnapshotAsOfTime(tableId: TableIdentifier, timestamp: Instant): SnapshotTimestamp

  /**
   * @param tableId
   * @param snapshotId the snapshot to start from. if none, start from the beginning
   * @param n
   * @throws IllegalArgumentException the provided snapshot does not exist in the history
   * @return if there are at least N snapshots after the provided snapshot, the Nth snapshot is returned in a Some.
   *         Otherwise, None.
   */
  def getNextNthSnapshot(tableId: TableIdentifier, snapshotId: Option[Long], n: Int): Option[SnapshotTimestamp]

  /**
   * Currently this is used for metrics and does not require freshness. ie this should not call a table refresh
   *
   * @param tableId
   * @return the number of snapshots in the table
   */
  def getNumSnapshots(tableId: TableIdentifier): Int

  /**
   * Currently this is used for metrics and does not require freshness. ie this should not call a table refresh
   *
   * @param tableId
   * @return the index of the snapshot in the snapshot log
   */
  def getSnapshotIndexInLog(tableId: TableIdentifier, snapshotId: Long): Int
}

object IcebergSnapshotReaderImpl {
  val log = LoggerFactory.getLogger(IcebergSnapshotReaderImpl.getClass)
}

/**
 * Reads iceberg snapshot files to determine changes between snapshots
 */
class IcebergSnapshotReaderImpl(
    catalog: Catalog,
    maxDuration: FiniteDuration = 10.seconds,
    clock: Clock = Clock.systemUTC())
  extends IcebergSnapshotReader {
  import IcebergSnapshotReaderImpl._

  override def getAffectedPartitions(
      tableId: TableIdentifier,
      lastSeenSnapshotId: Long
  ): AffectedPartitions = {
    val table = catalog.loadTable(tableId)
    val currentSnapshotId = getCurrentSnapshotId(table)

    validateSnapshotIds(table, lastSeenSnapshotId, currentSnapshotId)

    val newSnapshots = DataPlatformLog.getSnapshots(table, lastSeenSnapshotId, currentSnapshotId)

    log.info(s"getAffectedPartitions: ${newSnapshots.size} snapshots to process for ${tableId}")

    getAffectedPartitionsForSnapshots(table, lastSeenSnapshotId, newSnapshots)
  }

  override def getAffectedPrimaryPartitions(
      tableId: TableIdentifier,
      lastSeenSnapshotId: Long,
      windowStart: Int,
      windowEnd: Int
  ): AffectedPartitions = {

    var start = System.currentTimeMillis()
    val table = catalog.loadTable(tableId)

    val snapshot = table.currentSnapshot()
    val currentSnapshotId = snapshot.snapshotId()
    var end = System.currentTimeMillis()
    val snapshotLoadTime = (end - start) / 1000

    val affectedPartitions = new java.util.LinkedHashSet[Partition]()
    val spec = table.spec
    val nestedField = spec.partitionType.fields.get(0)
    val transform = spec.fields.get(0).transform

    val `type` = nestedField.`type`
    var curSnapshot = currentSnapshotId

    var snapshotsProcessed = 0L
    var lastSnapshot: Snapshot = null
    var lastSnapshotId: Long = 0L

    if (lastSeenSnapshotId == currentSnapshotId) {
      return AffectedPartitions(Set.empty, currentSnapshotId, true, Instant.ofEpochMilli(snapshot.timestampMillis()))
    }
    val historyStart = clock.millis()
    val tableHistory = table.history().asScala
    val newSnapshots = tableHistory.reverse
      .takeWhile(entry => entry.snapshotId != lastSeenSnapshotId)
      .map(x => long2Long(x.snapshotId))
      .toSet
    val foundSnapshot = tableHistory.exists(entry => entry.snapshotId == lastSeenSnapshotId)
    val historyTime = (clock.millis() - historyStart) / 1000

    if (!foundSnapshot && lastSeenSnapshotId != IcebergSnapshotReader.NoSnapshotId) {
      throw new NoSuchElementException(
        s"fromSnapshotId not found in table history: snapshot=$lastSeenSnapshotId, table=$table"
      )
    }

    start = System.currentTimeMillis()
    // this step takes ~ 3 sec
    val manifests =
      snapshot
        .allManifests()
        .iterator()
        .asScala
        .filter(mf => newSnapshots.contains(mf.snapshotId()))
        .toSeq
        .reverseIterator
    end = System.currentTimeMillis()
    val manifestIterationTime = (end - start) / 1000
    val startTime = clock.millis()
    val endTime = startTime + maxDuration.toMillis

    while (manifests.hasNext && clock.millis() < endTime) {
      val manifestFile = manifests.next()

      val summary = manifestFile.partitions.get(0)
      val lower: java.lang.Object = Conversions.fromByteBuffer(`type`, summary.lowerBound)
      val upper: java.lang.Object = Conversions.fromByteBuffer(`type`, summary.upperBound)
      val lowerInt = lower.asInstanceOf[java.lang.Integer]
      val upperInt = upper.asInstanceOf[java.lang.Integer]
      lastSnapshotId = manifestFile.snapshotId()
      if ((lowerInt >= windowStart && lowerInt <= windowEnd) || (upperInt <= windowEnd && upperInt >= windowStart)) {
        val start = System.currentTimeMillis()
        val partitions = DPManifestUtils.processManifest(table, manifestFile, newSnapshots.asJava)
        partitions.asScala
          .map(partition => convertToPartition(partition, table.spec()))
          .foreach(p => affectedPartitions.add(p))
        val end = System.currentTimeMillis()
        log.warn(s"loading added files took ${(end - start) / 1000}")
      }
      snapshotsProcessed += 1
    }
    lastSnapshot = table.snapshot(lastSnapshotId)
    val lastProcessedSnapshotTime =
      if (lastSnapshot != null && lastSnapshotId != IcebergSnapshotReader.NoSnapshotId)
        Instant.ofEpochMilli(lastSnapshot.timestampMillis())
      else
        Instant.EPOCH
    val loopEndTime = clock.millis()
    val totalTime = (loopEndTime - startTime) / 1000
    val isCompleted = !manifests.hasNext

    if (!isCompleted) {
      log.warn(s"getAffectedPrimaryPartitions: found ${affectedPartitions.size()} late partitions, Stopped before processing all new snapshots. Processed $snapshotsProcessed out of ${manifests.size}, " +
        s"Last snapshot time: $lastProcessedSnapshotTime. took ${totalTime} secs, snapshot load took ${snapshotLoadTime} secs, manifest iterator took ${manifestIterationTime}")
    } else {
      log.warn(s"getAffectedPrimaryPartitions: completed. Found ${affectedPartitions
        .size()} late partitions, Processed $snapshotsProcessed Last snapshot time: $lastProcessedSnapshotTime. took ${totalTime} secs" +
        s" , snapshot load took ${snapshotLoadTime} secs, table history took ${historyTime} manifest iterator took ${manifestIterationTime}")
    }
    AffectedPartitions(affectedPartitions.asScala.toSet, lastSnapshotId, isCompleted, lastProcessedSnapshotTime)
  }

  private def getAffectedPartitionsForSnapshots(
      table: Table,
      lastSeenSnapshotId: Long,
      snapshots: List[Long]): AffectedPartitions = {
    val startTime = clock.millis()
    val endTime = startTime + maxDuration.toMillis

    val affectedPartitions = mutable.Set.empty[Partition]
    var lastSnapshotExamined = lastSeenSnapshotId
    var snapshotsProcessed = 0

    val snapshotIterator = snapshots.iterator

    while (snapshotIterator.hasNext && clock.millis() < endTime) {
      lastSnapshotExamined = snapshotIterator.next()
      affectedPartitions ++= DataPlatformLog.getAffectedValues(table, lastSnapshotExamined) { file: DataFile =>
        convertToPartition(file.partition(), table.spec())
      }
      snapshotsProcessed += 1
    }

    val lastProcessedSnapshotTime =
      if (lastSnapshotExamined != IcebergSnapshotReader.NoSnapshotId)
        Instant.ofEpochMilli(table.snapshot(lastSnapshotExamined).timestampMillis())
      else
        Instant.EPOCH

    if (snapshotIterator.hasNext) {
      log.warn(
        s"getAffectedPartitions: Stopped before processing all new snapshots. Processed $snapshotsProcessed out of ${snapshots.size} Last snapshot time: $lastProcessedSnapshotTime")
    }

    AffectedPartitions(
      affectedPartitions.toSet,
      lastSnapshotExamined,
      !snapshotIterator.hasNext,
      lastProcessedSnapshotTime)
  }

  override def getCurrentSnapshot(tableId: TableIdentifier): SnapshotTimestamp = {
    val table = catalog.loadTable(tableId)
    Option(table.currentSnapshot())
      .map { snapshot =>
        SnapshotTimestamp(snapshot.snapshotId, Instant.ofEpochMilli(snapshot.timestampMillis()))
      }
      .getOrElse(IcebergSnapshotReader.NoSnapshot)
  }

  override def getSnapshotAsOfTime(tableId: TableIdentifier, timestamp: Instant): SnapshotTimestamp = {
    val table = catalog.loadTable(tableId)
    table.history.asScala.foldLeft(IcebergSnapshotReader.NoSnapshot) {
      case (acc, snapshot) =>
        if (snapshot.timestampMillis() <= timestamp.toEpochMilli) {
          SnapshotTimestamp(snapshot.snapshotId(), Instant.ofEpochMilli(snapshot.timestampMillis()))
        } else {
          acc
        }
    }
  }

  override def getNextNthSnapshot(
      tableId: TableIdentifier,
      snapshotId: Option[Long],
      n: Int): Option[SnapshotTimestamp] = {
    val table = catalog.loadTable(tableId)

    val snapshotLog = table.history().asScala

    /**
     * TODO(jclara) given the timestamp, we should be able to use Collections.binarySearch to find the snapshot in
     * O(log(N)) instead of log(n) since the table.history list is a RandomAccess list.
     */
    val i = snapshotId
      .map { id =>
        val i = snapshotLog.indexWhere(_.snapshotId() == id)
        if (i == -1) {
          throw new IllegalArgumentException("provided snapshot does not exist")
        }
        i
      }
      .getOrElse(-1)
    val historyEntry = if (i + n < snapshotLog.size) {
      Some(snapshotLog(i + n))
    } else {
      None
    }
    historyEntry.map(he => SnapshotTimestamp(he.snapshotId(), Instant.ofEpochMilli(he.timestampMillis())))
  }

  private def getCurrentSnapshotId(table: Table): Long =
    Option(table.currentSnapshot()).map { _.snapshotId }.getOrElse(IcebergSnapshotReader.NoSnapshotId)

  @tailrec
  private def convertToPartition(
      partition: StructLike,
      partitionSpec: PartitionSpec,
      field: Int = 0,
      resultPartition: Partition = Partition.empty
  ): Partition =
    if (field >= partition.size()) {
      resultPartition
    } else {
      val cls = partitionSpec.javaClasses()(field)
      val fieldName = partitionSpec.fields().asScala(field).name()
      val newField = PartitionField(fieldName, partition.get(field, cls))
      convertToPartition(
        partition,
        partitionSpec,
        field + 1,
        resultPartition.addField(newField)
      )
    }

  private def validateSnapshotIds(
      table: Table,
      fromSnapshotId: Long,
      toSnapshotId: Long
  ): Unit = {
    val snapshots = table.snapshots().asScala.toList.map(_.snapshotId())
    val fromSnapshotIndex = snapshots.indexOf(fromSnapshotId)
    val toSnapshotIndex = snapshots.indexOf(toSnapshotId)

    if (fromSnapshotId != IcebergSnapshotReader.NoSnapshotId) {
      if (fromSnapshotIndex == -1) {
        throw new NoSuchElementException(
          s"fromSnapshotId not found in table history: snapshot=$fromSnapshotId, table=$table"
        )
      }
    }

    if (toSnapshotId != IcebergSnapshotReader.NoSnapshotId) {
      if (toSnapshotIndex == -1) {
        throw new NoSuchElementException(
          s"toSnapshotId not found in table history: snapshot=$toSnapshotId, table=$table"
        )
      }
    }

    if (fromSnapshotId != IcebergSnapshotReader.NoSnapshotId) {
      if (toSnapshotId == IcebergSnapshotReader.NoSnapshotId) {
        throw new IllegalArgumentException(
          s"trying to read from a specific snapshot to the null snapshot: from=$fromSnapshotId, to=$toSnapshotId, table=$table"
        )
      } else if (fromSnapshotIndex > toSnapshotIndex) {
        throw new IllegalArgumentException(
          s"fromSnapshotId is chronologically later than toSnapshotId: from=$fromSnapshotId, to=$toSnapshotId, table=$table"
        )
      }
    }
  }

  /**
   * Currently this is used for metrics and does not require freshness. ie this should not call a table refresh
   *
   * @param tableId
   * @return the number of snapshots in the table
   */
  override def getNumSnapshots(tableId: TableIdentifier): Int = {
    val table = catalog.loadTable(tableId)
    table.history().asScala.size
  }

  /**
   * Currently this is used for metrics and does not require freshness. ie this should not call a table refresh
   *
   * @param tableId
   * @return the index of the snapshot in the snapshot log
   */
  override def getSnapshotIndexInLog(tableId: TableIdentifier, snapshotId: Long): Int = {
    val table = catalog.loadTable(tableId)
    table.history().asScala.foldLeft((0, false)) {
      case ((i, found), he) =>
        if (found || he.snapshotId() == snapshotId) {
          (i, true)
        } else {
          (i + 1, false)
        }
    } match {
      case (i, true) =>
        i
      case (i, false) =>
        throw new IllegalArgumentException(s"Snapshot $snapshotId does not exist in the log")
    }
  }
}
