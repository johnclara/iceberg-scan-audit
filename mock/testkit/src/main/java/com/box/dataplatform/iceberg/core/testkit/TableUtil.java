package com.box.dataplatform.iceberg.core.testkit;

import org.apache.iceberg.catalog.TableIdentifier;

import java.util.UUID;

import static com.box.dataplatform.iceberg.core.testkit.TestkitUtil.uniquify;

public class TableUtil {
  public static final String NAMESPACE = "mytenant";
  public static final String DEFAULT_TABLE_PREFIX = "table";
  public static TableIdentifier newTableId() {
    return newTableId(DEFAULT_TABLE_PREFIX);
  }

  public static TableIdentifier newTableId(String tablePrefix) {
    String tableGuid = UUID.randomUUID().toString().replaceAll("-", "_");
    return TableIdentifier.of(NAMESPACE, uniquify(tablePrefix));
  }
}
