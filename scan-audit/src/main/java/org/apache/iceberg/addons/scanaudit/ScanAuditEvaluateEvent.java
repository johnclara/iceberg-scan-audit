package org.apache.iceberg.addons.scanaudit;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

public final class ScanAuditEvaluateEvent implements ScanAuditEvent {
  private final String tableName;
  private final long scanId;
  private final String parentPath;
  private final String childPath;
  private final EvaluatorType evaluatorType;
  private final Boolean passedEvaluate; //ie it was not pruned

  public Record toRecord(Schema schema) {
    GenericRecord result = GenericRecord.create(schema);
    result.setField("tableName", tableName);
    result.setField("scanId", scanId);
    result.setField("parentPath", parentPath);
    result.setField("childPath", childPath);
    result.setField("evaluatorType", evaluatorType.name());
    result.setField("passedEvaluate", passedEvaluate);
    return result;
  }

  private ScanAuditEvaluateEvent(String tableName, long scanId, String parentPath, String childPath, EvaluatorType evaluatorType, Boolean passedEvaluate) {
    this.tableName = tableName;
    this.scanId = scanId;
    this.parentPath = parentPath;
    this.childPath = childPath;
    this.evaluatorType = evaluatorType;
    this.passedEvaluate = passedEvaluate;
  }

  public static ScanAuditEvaluateEvent of(String tableName, long scanId, String parentPath, String childPath, EvaluatorType evaluatorType, Boolean passedEvaluate) {
    return new ScanAuditEvaluateEvent(tableName, scanId, parentPath, childPath, evaluatorType, passedEvaluate);
  }

  public String tableName() {
    return tableName;
  }

  public long scanId() {
    return scanId;
  }

  public String parentPath() {
    return parentPath;
  }

  public String childPath() {
    return childPath;
  }

  public EvaluatorType evaluatorType() {
    return evaluatorType;
  }

  public Boolean passedEvaluate() {
    return passedEvaluate;
  }

  @Override
  public String toString() {
    return "ScanAuditEvaluateEvent{" +
        "tableName='" + tableName + '\'' +
        ", scanId=" + scanId +
        ", parentPath='" + parentPath + '\'' +
        ", childPath='" + childPath + '\'' +
        ", evaluatorType=" + evaluatorType +
        ", passedEvaluate=" + passedEvaluate +
        '}';
  }
}
