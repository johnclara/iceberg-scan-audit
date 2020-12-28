package org.apache.iceberg;

import org.apache.iceberg.addons.scanaudit.ScanAuditEndEvent;
import org.apache.iceberg.addons.scanaudit.ScanAuditEvaluateEvent;
import org.apache.iceberg.addons.scanaudit.ScanAuditStartEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.ThreadPools;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public class AuditedDataTableScan extends DataTableScan {
  public AuditedDataTableScan(TableOperations ops, Table table) {
    super(ops, table);
  }

  protected AuditedDataTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }


  @Override
  protected AuditedDataTableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new AuditedDataTableScan(ops, table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot,
                                                   Expression rowFilter, boolean ignoreResiduals,
                                                   boolean caseSensitive, boolean colStats) {


    long scanId = ops.newSnapshotId();
    Listeners.notifyAll(
        ScanAuditStartEvent.of(
            table().name(),
            scanId
        )
    );

    AuditingManifestGroup auditingManifestGroup = new AuditingManifestGroup(
          table().name(),
          scanId,
          snapshot.manifestListLocation(),
          ops.io(),
          snapshot.dataManifests(),
          snapshot.deleteManifests()
        )
        .caseSensitive(caseSensitive)
        .select(colStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
        .filterData(rowFilter)
        .specsById(ops.current().specsById())
        .ignoreDeleted();

    if (ignoreResiduals) {
      auditingManifestGroup = auditingManifestGroup.ignoreResiduals();
    }

    if (PLAN_SCANS_WITH_WORKER_POOL && snapshot.dataManifests().size() > 1) {
      auditingManifestGroup = auditingManifestGroup.planWith(ThreadPools.getWorkerPool());
    }

    // TODO added event
    CloseableIterable<FileScanTask> fileScanTasks = auditingManifestGroup.planFiles();
    return new CloseableIterable<FileScanTask>() {
      private final CloseableIterable<FileScanTask> delegateIterable = fileScanTasks;
      @Override
      public CloseableIterator<FileScanTask> iterator() {
        final CloseableIterator<FileScanTask> delegateIterator = delegateIterable.iterator();
        return new CloseableIterator<FileScanTask>() {
          @Override
          public void close() throws IOException {
            delegateIterator.close();
          }

          @Override
          public boolean hasNext() {
            boolean delHasNext = delegateIterator.hasNext();
            if (!delHasNext) {
              Listeners.notifyAll(
                ScanAuditEndEvent.of(
                    table().name(),
                    scanId
                )
              );
            }
            return delHasNext;
          }

          @Override
          public FileScanTask next() {
            return delegateIterator.next();
          }
        };
      }

      @Override
      public void close() throws IOException {
        fileScanTasks.close();
      }
    };
    // TODO added event
  }

}
