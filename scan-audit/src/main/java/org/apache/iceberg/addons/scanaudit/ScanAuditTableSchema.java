package org.apache.iceberg.addons.scanaudit;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;


public class ScanAuditTableSchema extends Schema {
  public static ScanAuditTableSchema INSTANCE = new ScanAuditTableSchema();
  protected ScanAuditTableSchema(){
    super(
        // TODO time partition
        Types.NestedField.of(0, false, "tableName", Types.StringType.get()),
        Types.NestedField.of(1, false, "scanId", Types.LongType.get()),
        Types.NestedField.of(2, true, "parentPath", Types.StringType.get()),
        Types.NestedField.of(3, false, "childPath", Types.StringType.get()),
        Types.NestedField.of(4, false, "evaluatorType", Types.StringType.get()),
        Types.NestedField.of(5, false, "passedEvaluate", Types.BooleanType.get())
    );
  }
}
