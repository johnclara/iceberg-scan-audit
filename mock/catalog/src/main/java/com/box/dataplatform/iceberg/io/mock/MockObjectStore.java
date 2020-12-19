package com.box.dataplatform.iceberg.io.mock;

import com.box.dataplatform.iceberg.io.ObjectStore;
import com.box.dataplatform.iceberg.mock.MockContextKey;

import java.util.concurrent.ConcurrentHashMap;

public class MockObjectStore implements ObjectStore {
  private final ConcurrentHashMap<String, MockFileObject> fileObjects;
  private final MockContextKey contextKey;
  public MockObjectStore(MockContextKey contextKey) {
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
