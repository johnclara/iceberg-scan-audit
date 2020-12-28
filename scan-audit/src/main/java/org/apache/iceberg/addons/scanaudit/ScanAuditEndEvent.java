package org.apache.iceberg.addons.scanaudit;

public final class ScanAuditEndEvent implements ScanAuditEvent {
  private final String tableName;
  private final long scanId;

  private ScanAuditEndEvent(String tableName, long scanId) {
    this.scanId = scanId;
    this.tableName = tableName;
  }

  public static ScanAuditEndEvent of(String tableName, long scanId) {
    return new ScanAuditEndEvent(tableName, scanId);
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
    return "ScanAuditEndEvent{" +
        "tableName='" + tableName + '\'' +
        ", scanId=" + scanId +
        '}';
  }
}
