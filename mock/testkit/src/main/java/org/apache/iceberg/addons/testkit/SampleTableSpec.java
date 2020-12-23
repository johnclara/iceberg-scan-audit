package org.apache.iceberg.addons.testkit;

import org.apache.iceberg.Table;
import org.apache.iceberg.addons.mock.MockCatalog;
import org.apache.iceberg.addons.mock.MockContextId;
import org.apache.iceberg.addons.cataloglite.tablespec.TableSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

public abstract class SampleTableSpec {
  protected abstract String namePrefix();
  public abstract TableSpec spec();

  public TableIdentifier newId() {
    return TableIdentifierUtil.newTableId(namePrefix());
  }

  public Table create(TableIdentifier tableId, Catalog catalog) {
    return spec()
        .withTableId(tableId)
        .create(catalog);
  }

  public Table create(TableIdentifier tableId, MockContextId mockContextId) {
    Catalog catalog = new MockCatalog(mockContextId);
    return create(tableId, catalog);
  }
}
