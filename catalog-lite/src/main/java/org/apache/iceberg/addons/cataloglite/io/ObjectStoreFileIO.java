package org.apache.iceberg.addons.cataloglite.io;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class ObjectStoreFileIO implements FileIO {
  private final ObjectStore objectStore;

  public ObjectStoreFileIO(ObjectStore objectStore) {
    this.objectStore = objectStore;
  }

  @Override
  public InputFile newInputFile(String path) {
    return objectStore.getFileObject(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return objectStore.getFileObject(path);
  }

  @Override
  public void deleteFile(String path) {
    objectStore.deleteFileObject(path);
  }
}
