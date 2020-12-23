package org.apache.iceberg.addons.mock;

import org.apache.iceberg.addons.cataloglite.CatalogLite;
import org.apache.iceberg.addons.cataloglite.io.ObjectStore;
import org.apache.iceberg.addons.cataloglite.metastore.Metastore;

public class MockCatalog extends CatalogLite {
  public MockCatalog(Metastore metastore, ObjectStore objectStore) {
    super(metastore, objectStore);
  }

  public MockCatalog(MockContextId contextKey) {
    this(MockContext.getContext(contextKey));
  }

  protected MockCatalog(MockContext context) {
    this(context.metastore(), context.objectStore());
  }
}
