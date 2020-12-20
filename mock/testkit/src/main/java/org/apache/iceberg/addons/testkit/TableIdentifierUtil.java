package org.apache.iceberg.addons.testkit;

import org.apache.iceberg.catalog.TableIdentifier;

import java.util.UUID;

import static org.apache.iceberg.addons.testkit.Util.uniquify;

public class TableIdentifierUtil {
  public static final String NAMESPACE = "mynamespace";
  public static final String DEFAULT_TABLE_PREFIX = "table";
  public static TableIdentifier newTableId() {
    return newTableId(DEFAULT_TABLE_PREFIX);
  }

  public static TableIdentifier newTableId(String tablePrefix) {
    return TableIdentifier.of(NAMESPACE, uniquify(tablePrefix));
  }
}
