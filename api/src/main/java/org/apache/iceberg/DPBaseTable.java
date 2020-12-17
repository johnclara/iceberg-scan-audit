package org.apache.iceberg;

import com.box.dataplatform.iceberg.DPTableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class DPBaseTable extends BaseTable {
  private static final Logger log = LoggerFactory.getLogger(DPBaseTable.class.getName());
  private final TableOperations ops;
  private Long fromSnapshotId;
  private Long toSnapshotId;
  private boolean dedupeFiles;

  public DPBaseTable(TableOperations ops, String name) {
    super(ops, name);
    this.ops = ops;

    // setting these for clarity
    this.fromSnapshotId = null;
    this.toSnapshotId = null;
    String readDedupeFiles = properties().get(DPTableProperties.READ_DEDUPE_FILES);
    this.dedupeFiles = readDedupeFiles != null && Boolean.valueOf(readDedupeFiles);
    log.info("Initializing table: {}", name);
  }

  @Override
  public TableScan newScan() {
    log.info("Creating new scan");
    DPDataTableScan dpScan = new DPDataTableScan(ops, this);
    TableScan result;

    if (dedupeFiles) {
      log.info("Using dedupe files");
      result = dpScan.dedupeFiles();
    } else {
      result = dpScan;
    }
    if (fromSnapshotId != null && toSnapshotId != null) {
      log.info("Using from snapshotId: {} to snapshotId: {}", fromSnapshotId, toSnapshotId);
      result = result.appendsBetween(fromSnapshotId, toSnapshotId);
    } else if (fromSnapshotId != null) {
      log.info("Using from snapshotId: {}", fromSnapshotId);
      result = result.appendsAfter(fromSnapshotId);
    } else if (toSnapshotId != null) {
      log.info("iUsng to snapshotId: {}", toSnapshotId);
      result = result.useSnapshot(toSnapshotId);
    }
    return result;
  }

  public void withFromSnapshotId(Long fromSnapshotId) {
    this.fromSnapshotId = fromSnapshotId;
  }

  public void withToSnapshotId(Long toSnapshotId) {
    this.toSnapshotId = toSnapshotId;
  }

  public void withDedupeFiles(boolean dedupeFiles) {
    this.dedupeFiles = dedupeFiles;
  }
}
