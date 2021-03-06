package org.apache.iceberg.addons.cataloglite;

import org.apache.iceberg.addons.cataloglite.io.ObjectStoreFileIO;
import org.apache.iceberg.addons.cataloglite.io.ObjectStore;
import org.apache.iceberg.addons.cataloglite.metastore.Metastore;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

public class TableOperationsLite extends BaseMetastoreTableOperations {
  private final TableIdentifier tableId;
  private final Metastore metastore;
  private final ObjectStore objectStore;
  private FileIO fileIO = null;

  protected TableOperationsLite(
      TableIdentifier tableId,
      Metastore metastore,
      ObjectStore objectStore) {
    this.tableId = tableId;
    this.metastore = metastore;
    this.objectStore = objectStore;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new ObjectStoreFileIO(objectStore);
    }
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = metastore.getMetadataForTable(tableId);
    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String oldMetadataLocation = currentMetadataLocation();
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    metastore.updateMetadataLocation(tableId, oldMetadataLocation, newMetadataLocation);
  }
}
