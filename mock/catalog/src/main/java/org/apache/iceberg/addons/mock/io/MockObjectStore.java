package org.apache.iceberg.addons.mock.io;

import org.apache.iceberg.addons.cataloglite.io.ObjectStore;
import org.apache.iceberg.addons.mock.MockContextId;

import java.util.concurrent.ConcurrentHashMap;

public class MockObjectStore implements ObjectStore {
  private final ConcurrentHashMap<String, MockFileObject> fileObjects;
  private final MockContextId contextKey;
  public MockObjectStore(MockContextId contextKey) {
    this.fileObjects = new ConcurrentHashMap<>();
    this.contextKey = contextKey;
  }

  public MockFileObject getFileObject(String path) {
    return fileObjects.computeIfAbsent(path, k -> new MockFileObject(k));
  }

  public MockFileObject deleteFileObject(String path) {
    return fileObjects.remove(path);
  }
}
