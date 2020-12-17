package org.apache.iceberg;

import static org.apache.iceberg.DPDataTableScan.DEDUPE_FILES_OPTION;

import java.util.*;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation is replacement for {@link IncrementalDataTableScan}, which seems to be very
 * expensive as it loads manifest for every snapshot in the range. IncrementalDataTableScanLite
 * implementation instead reads manifest only once and reads all files in the range.
 */
class DPIncrementalDataTableScan extends IncrementalDataTableScan {
  private static final Logger log = LoggerFactory.getLogger(DPIncrementalDataTableScan.class);

  DPIncrementalDataTableScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  public TableScan appendsBetween(long newFromSnapshotId, long newToSnapshotId) {
    validateSnapshotIdsRefinement(newFromSnapshotId, newToSnapshotId);
    return new DPIncrementalDataTableScan(
        tableOps(),
        table(),
        schema(),
        context().fromSnapshotId(newFromSnapshotId).toSnapshotId(newToSnapshotId));
  }

  private Set<Long> getSnapshotsToProcess() {
    int index = 0;
    Set<Long> snapshotsToProcess = new HashSet<Long>();
    Iterator<HistoryEntry> it = super.table().history().iterator();
    boolean seenFrom = false;
    boolean seenTo = false;
    Long fromSnapshotId = context().fromSnapshotId();
    Long toSnapshotId = context().toSnapshotId();
    // History is in chronological order
    while (it.hasNext() && !seenTo) {
      HistoryEntry e = it.next();
      if (seenFrom) {
        snapshotsToProcess.add(e.snapshotId());
      }
      if (e.snapshotId() == fromSnapshotId) {
        log.info("Found start timestamp");
        seenFrom = true;
      }
      if (e.snapshotId() == toSnapshotId) {
        log.info("Found final timestamp");
        seenTo = true;
      }
    }
    return snapshotsToProcess;
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    log.info("Starting plan files");
    Set<ManifestFile> manifests = new java.util.HashSet<>();
    List<ManifestFile> mfi = super.table().currentSnapshot().allManifests();
    manifests.addAll(mfi);
    log.info("Got all manifest files");
    Set<Long> snapshotsToProcess = getSnapshotsToProcess();
    log.info("Got all snapshots to process");

    ManifestGroup manifestGroup =
        new ManifestGroup(tableOps().io(), manifests)
            .caseSensitive(isCaseSensitive())
            .select(colStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
            .filterData(filter())
            .filterManifests(manifest -> snapshotsToProcess.contains(manifest.snapshotId()))
            .filterManifestEntries(
                manifestEntry -> snapshotsToProcess.contains(manifestEntry.snapshotId()))
            .specsById(tableOps().current().specsById())
            .ignoreDeleted();

    log.info("Created manifest group");

    if (PLAN_SCANS_WITH_WORKER_POOL && manifests.size() > 1) {
      manifestGroup = manifestGroup.planWith(ThreadPools.getWorkerPool());
    }

    CloseableIterable<FileScanTask> fileScanTasks = manifestGroup.planFiles();
    log.info("Planned files");
    if (context().options().containsKey(DEDUPE_FILES_OPTION)) {
      log.info("Adding dedupe wrapper");
      return new DedupingIterable<>(fileScanTasks, f -> f.file().path());
    } else {
      return fileScanTasks;
    }
  }

  @Override
  protected TableScan newRefinedScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new DPIncrementalDataTableScan(ops, table, schema, context);
  }

  private void validateSnapshotIdsRefinement(long newFromSnapshotId, long newToSnapshotId) {
    Set<Long> snapshotIdsRange =
        Sets.newHashSet(
            SnapshotUtil.snapshotIdsBetween(
                table(), context().fromSnapshotId(), context().toSnapshotId()));
    // since snapshotIdsBetween return ids in range (fromSnapshotId, toSnapshotId]
    snapshotIdsRange.add(context().fromSnapshotId());
    org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkArgument(
        snapshotIdsRange.contains(newFromSnapshotId),
        "from snapshot id %s not in existing snapshot ids range (%s, %s]",
        newFromSnapshotId,
        context().fromSnapshotId(),
        newToSnapshotId);
    org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkArgument(
        snapshotIdsRange.contains(newToSnapshotId),
        "to snapshot id %s not in existing snapshot ids range (%s, %s]",
        newToSnapshotId,
        context().fromSnapshotId(),
        context().toSnapshotId());
  }
}
