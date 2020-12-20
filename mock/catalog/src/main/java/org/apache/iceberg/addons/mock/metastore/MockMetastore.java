package org.apache.iceberg.addons.mock.metastore;

import org.apache.iceberg.addons.cataloglite.metastore.Metastore;
import org.apache.iceberg.addons.mock.MockContextId;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MockMetastore implements Metastore {
  private static final Logger logger = LoggerFactory.getLogger(MockMetastore.class);

  private final MockContextId contextKey;
  private final ConcurrentHashMap<TableIdentifier, String> mockStore;

  public MockMetastore(MockContextId contextKey) {
    this.contextKey = contextKey;
    this.mockStore = new ConcurrentHashMap<>();
  }

  @Override
  public String getMetadataForTable(TableIdentifier tableId) {
    return mockStore.get(tableId);
  }

  @Override
  public void updateMetadataLocation(TableIdentifier tableId, String oldMetadataLocation, String newMetadataLocation) {
    mockStore.compute(tableId, (id, currLocation) -> {
      if ((currLocation == null) != (oldMetadataLocation == null)) {
        throw new IllegalStateException(
            String.format("Either current %s xor expected %s is null", tableId.toString(), oldMetadataLocation)
        );
      } else if (currLocation != null && oldMetadataLocation != null && !currLocation.equals(oldMetadataLocation)) {
        throw new CommitFailedException(
            String.format("Table %s had previous metadata location but required %s", tableId.toString(), currLocation, oldMetadataLocation)
        );
      } else {
        return newMetadataLocation;
      }
    });
  }

  @Override
  public boolean deleteTable(TableIdentifier tableId) {
    return mockStore.remove(tableId) != null;
  }

  @Override
  public boolean renameTable(TableIdentifier oldTableId, TableIdentifier newTableId) {
    throw new IllegalArgumentException("Unsupported");
  }
}
