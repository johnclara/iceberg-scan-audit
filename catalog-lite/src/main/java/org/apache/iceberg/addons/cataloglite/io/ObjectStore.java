package org.apache.iceberg.addons.cataloglite.io;

import java.io.Serializable;

public interface ObjectStore extends Serializable {
  FileObject getFileObject(String path);
  FileObject deleteFileObject(String path);
}
