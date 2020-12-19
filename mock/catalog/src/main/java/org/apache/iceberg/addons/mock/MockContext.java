package org.apache.iceberg.addons.mock;

import org.apache.iceberg.addons.io.mock.MockObjectStore;
import org.apache.iceberg.addons.metastore.mock.MockMetastore;

import java.io.Serializable;

public class MockContext implements Serializable {
  private final MockContextKey contextKey;
  private final MockObjectStore objectStore;
  private final MockMetastore metastore;

  public MockContext(MockContextKey contextKey, MockObjectStore objectStore, MockMetastore metastore) {
    this.contextKey = contextKey;
    this.objectStore = objectStore;
    this.metastore = metastore;
  }

  public MockObjectStore objectStore() {
    return objectStore;
  }

  public MockMetastore metastore() {
    return metastore;
  }
}
