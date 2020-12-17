package org.apache.iceberg;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DPDataTableScan extends DataTableScan {
  private static final Logger log = LoggerFactory.getLogger(DPDataTableScan.class);
  public static final String DEDUPE_FILES_OPTION = "dedupe_files";

  public DPDataTableScan(TableOperations ops, Table table) {
    super(ops, table);
  }

  protected DPDataTableScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    Long scanSnapshotId = snapshotId();
    Preconditions.checkState(
        scanSnapshotId == null,
        "Cannot enable incremental scan, scan-snapshot set to id=%s",
        scanSnapshotId);
    return new DPIncrementalDataTableScan(
        tableOps(),
        table(),
        schema(),
        context().fromSnapshotId(fromSnapshotId).toSnapshotId(toSnapshotId));
  }

  @Override
  protected TableScan newRefinedScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new DPDataTableScan(ops, table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    log.info("Starting plan files");
    CloseableIterable<FileScanTask> fileScanTasks = super.planFiles();
    log.info("Planned files");
    if (context().options().containsKey(DEDUPE_FILES_OPTION)) {
      log.info("Adding dedupe wrapper");
      return new DedupingIterable<>(fileScanTasks, f -> f.file().path());
    } else {
      return fileScanTasks;
    }
  }

  public TableScan dedupeFiles() {
    return newRefinedScan(
        tableOps(), table(), schema(), context().withOption(DEDUPE_FILES_OPTION, "true"));
  }
}
