package com.box.dataplatform.iceberg.metastore.mock;

import com.box.dataplatform.iceberg.metastore.Metastore;
import com.box.dataplatform.iceberg.mock.MockContextKey;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MockMetastore implements Metastore {
  private static final Logger logger = LoggerFactory.getLogger(MockMetastore.class);

  private final MockContextKey contextKey;
  private final ConcurrentHashMap<TableIdentifier, String> mockStore;

  public MockMetastore(MockContextKey contextKey) {
    this.contextKey = contextKey;
    this.mockStore = new ConcurrentHashMap<>();
  }

  @Override
  public String getMetadataForTable(TableIdentifier tableId) {
    return mockStore.get(tableId.toString());
  }

  @Override
  public void updateMetadataLocation(TableIdentifier tableId, String oldMetadataLocation, String newMetadataLocation) {
    mockStore.replace(tableId, oldMetadataLocation, newMetadataLocation);
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
