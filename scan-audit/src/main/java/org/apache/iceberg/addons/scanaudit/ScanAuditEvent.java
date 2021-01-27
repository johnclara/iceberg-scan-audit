package org.apache.iceberg.addons.scanaudit;

import java.io.Serializable;

public interface ScanAuditEvent extends Serializable {
  String tableName();
  long scanId();
}
