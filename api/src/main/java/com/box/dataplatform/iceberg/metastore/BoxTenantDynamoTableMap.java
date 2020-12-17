package com.box.dataplatform.iceberg.metastore;

import java.util.Arrays;

public class BoxTenantDynamoTableMap implements TenantDynamoTableMap {
  private BoxTenantDynamoTableMap() {}

  public static final BoxTenantDynamoTableMap INSTANCE = new BoxTenantDynamoTableMap();

  public String getDynamoTableName(String tenantName) {
    return Arrays.stream(tenantName.split("-"))
            .map(
                s -> {
                  char c = s.charAt(0);
                  return Character.toUpperCase(c) + s.substring(1);
                })
            .reduce("", (l, r) -> l + r)
        + "IcebergMetadata";
  }
}
