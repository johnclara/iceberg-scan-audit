package com.box.dataplatform.iceberg.metastore;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestBoxTenantDynamoTableMap {
  @Test
  public void testGetDynamoTableName() {
    assertEquals(
        "ShieldIcebergMetadata", BoxTenantDynamoTableMap.INSTANCE.getDynamoTableName("shield"));
    assertEquals(
        "AuditLogSandboxIcebergMetadata",
        BoxTenantDynamoTableMap.INSTANCE.getDynamoTableName("audit-log-sandbox"));
  }
}
