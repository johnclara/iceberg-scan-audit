package com.box.dataplatform.iceberg.mock;

import com.box.dataplatform.iceberg.io.mock.MockObjectStore;
import com.box.dataplatform.iceberg.metastore.mock.MockMetastore;

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
