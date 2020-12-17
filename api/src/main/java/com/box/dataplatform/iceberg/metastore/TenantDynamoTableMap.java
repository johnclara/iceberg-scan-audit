package com.box.dataplatform.iceberg.metastore;

import java.io.Serializable;

public interface TenantDynamoTableMap extends Serializable {
  String getDynamoTableName(String tenantName);
}
