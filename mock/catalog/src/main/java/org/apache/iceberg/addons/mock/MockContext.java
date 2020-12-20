package org.apache.iceberg.addons.mock;

import org.apache.iceberg.addons.mock.io.MockObjectStore;
import org.apache.iceberg.addons.mock.metastore.MockMetastore;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class MockContext implements Serializable {
  public static final ConcurrentHashMap<MockContextId, MockContext> mockContexts = new ConcurrentHashMap<>();
  public static MockContext getContext(MockContextId contextId) {
    return MockContext.mockContexts.computeIfAbsent(contextId, (id) -> new MockContext(id, new MockObjectStore(id), new MockMetastore(id)));
  }

  private final MockContextId contextKey;
  private final MockObjectStore objectStore;
  private final MockMetastore metastore;

  public MockContext(MockContextId contextKey, MockObjectStore objectStore, MockMetastore metastore) {
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
