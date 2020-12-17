package com.box.dataplatform.iceberg

import java.time.{Instant, Period}

import org.apache.iceberg.{HistoryEntry, Snapshot, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.specs2.SpecificationWithJUnit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.mock.Mockito

import scala.collection.JavaConverters._
import scala.util.Try

class IcebergSnapshotReaderSpec extends SpecificationWithJUnit with Mockito with MustThrownMatchers {
  def is =
    s2"""
        An IcebergSnapshotReader should
          return the correct current Iceberg table snapshot
            when a current snapshot
              exists                                          ${env().testGetCurrentSnapshotIdExists}
              does not exist                                  ${env().testGetCurrentSnapshotIdNoSnapshot}
          return the snapshot before the timestamp
              when a snapshot
                exists                                        ${env().testSnapshotAsOfTimeExists}
                does not exist                                ${env().testSnapshotAsOfTimeNoSnapshot}
          get next nth snapshot     ${env().testNextNthSnapshot}
          get num snapshots         ${env().testGetNumSnapshots}
          get snapshot index in log ${env().testGetSnapshotIndexInLog}
     """

  case class env() {
    val catalog = mock[Catalog]
    val uut = new IcebergSnapshotReaderImpl(catalog)

    val tableIdentifier = mock[TableIdentifier]
    val table = mock[Table]

    catalog.loadTable(tableIdentifier).returns(table)

    def testGetCurrentSnapshotIdExists = {
      val snapshot = mock[Snapshot]
      snapshot.snapshotId().returns(12345L)
      table.currentSnapshot().returns(snapshot)

      uut.getCurrentSnapshot(tableIdentifier).snapshotId ==== 12345L
    }

    def testGetCurrentSnapshotIdNoSnapshot = {
      table.currentSnapshot().returns(null)

      uut.getCurrentSnapshot(tableIdentifier).snapshotId ==== IcebergSnapshotReader.NoSnapshotId
    }

    val time = Instant.ofEpochMilli(100)

    def testSnapshotAsOfTimeExists = {
      table
        .history()
        .returns(
          List(
            new HistoryEntry {
              override def timestampMillis(): Long = 98L

              override def snapshotId(): Long = 12344L
            },
            new HistoryEntry {
              override def timestampMillis(): Long = 99

              override def snapshotId(): Long = 12345L
            }
          ).asJava
        )

      uut.getSnapshotAsOfTime(tableIdentifier, time) ==== SnapshotTimestamp(12345L, Instant.ofEpochMilli(99))
    }

    def testNextNthSnapshot = {
      val s0 = new HistoryEntry {
        override def timestampMillis(): Long = 98L

        override def snapshotId(): Long = 12344L
      }
      val s1 = new HistoryEntry {
        override def timestampMillis(): Long = 100

        override def snapshotId(): Long = 12345L
      }
      val s2 = new HistoryEntry {
        override def timestampMillis(): Long = 1000

        override def snapshotId(): Long = 12346L
      }
      table
        .history()
        .returns(List(s0, s1, s2).asJava)

      def toSnapshotTimestamp(he: HistoryEntry): SnapshotTimestamp =
        SnapshotTimestamp(he.snapshotId(), Instant.ofEpochMilli(he.timestampMillis()))

      uut.getNextNthSnapshot(tableIdentifier, None, 1) ==== Some(toSnapshotTimestamp(s0))
      uut.getNextNthSnapshot(tableIdentifier, Some(12346), 1) ==== None
      uut.getNextNthSnapshot(tableIdentifier, Some(12344), 1) ==== Some(toSnapshotTimestamp(s1))
      uut.getNextNthSnapshot(tableIdentifier, Some(12344), 10) ==== None
      uut.getNextNthSnapshot(tableIdentifier, Some(-1), 1) must throwAn[IllegalArgumentException]
    }

    def testGetNumSnapshots = {
      val s0 = new HistoryEntry {
        override def timestampMillis(): Long = 98L

        override def snapshotId(): Long = 12344L
      }
      val s1 = new HistoryEntry {
        override def timestampMillis(): Long = 100

        override def snapshotId(): Long = 12345L
      }
      val s2 = new HistoryEntry {
        override def timestampMillis(): Long = 1000

        override def snapshotId(): Long = 12346L
      }
      table
        .history()
        .returns(List(s0, s1, s2).asJava)

      uut.getNumSnapshots(tableIdentifier) ==== 3
    }

    def testGetSnapshotIndexInLog = {
      val s0 = new HistoryEntry {
        override def timestampMillis(): Long = 98L

        override def snapshotId(): Long = 12344L
      }
      val s1 = new HistoryEntry {
        override def timestampMillis(): Long = 100

        override def snapshotId(): Long = 12345L
      }
      val s2 = new HistoryEntry {
        override def timestampMillis(): Long = 1000

        override def snapshotId(): Long = 12346L
      }
      table
        .history()
        .returns(List(s0, s1, s2).asJava)

      uut.getSnapshotIndexInLog(tableIdentifier, 12344L) ==== 0
      uut.getSnapshotIndexInLog(tableIdentifier, 12345L) ==== 1
      uut.getSnapshotIndexInLog(tableIdentifier, 12346L) ==== 2
      Try(uut.getSnapshotIndexInLog(tableIdentifier, -1)) must beAFailedTry
    }

    def testSnapshotAsOfTimeNoSnapshot = {
      table
        .history()
        .returns(
          List(
            new HistoryEntry {
              override def timestampMillis(): Long = 101

              override def snapshotId(): Long = 12344L
            },
            new HistoryEntry {
              override def timestampMillis(): Long = 102

              override def snapshotId(): Long = 12345L
            }
          ).asJava
        )

      uut.getSnapshotAsOfTime(tableIdentifier, time).snapshotId ==== IcebergSnapshotReader.NoSnapshotId
    }
  }
}
