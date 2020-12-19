package com.box.dataplatform.iceberg.io;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public interface ObjectStore extends Serializable {
  FileObject getFileObject(String path);
  FileObject deleteFileObject(String path);
}
