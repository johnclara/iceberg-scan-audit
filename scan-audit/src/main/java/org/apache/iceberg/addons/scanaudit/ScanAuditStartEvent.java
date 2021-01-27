package org.apache.iceberg.addons.scanaudit;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

public final class ScanAuditStartEvent implements ScanAuditEvent {
  private final String tableName;
  private final long scanId;

  private ScanAuditStartEvent(String tableName, long scanId) {
    this.tableName = tableName;
    this.scanId = scanId;
  }

  public static ScanAuditStartEvent of(String tableName, long scanId) {
    return new ScanAuditStartEvent(tableName, scanId);
  }

  @Override
  public String tableName() {
    return tableName;
  }

  @Override
  public long scanId() {
    return scanId;
  }

  @Override
  public String toString() {
    return "ScanAuditStartEvent{" +
        "tableName='" + tableName + '\'' +
        ", scanId=" + scanId +
        '}';
  }
}
