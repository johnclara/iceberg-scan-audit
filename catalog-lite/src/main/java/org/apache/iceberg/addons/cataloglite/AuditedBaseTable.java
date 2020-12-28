package org.apache.iceberg.addons.cataloglite;

import org.apache.iceberg.*;

public class AuditedBaseTable extends BaseTable {
  public AuditedBaseTable(TableOperations ops, String name) {
    super(ops, name);
  }


  @Override
  public TableScan newScan() {
    return new AuditedDataTableScan(operations(), this);
  }
}
