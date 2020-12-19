package com.box.dataplatform.iceberg.metastore;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.Serializable;
import java.util.List;

public interface Metastore extends Serializable {
  String getMetadataForTable(TableIdentifier tableId);

  void updateMetadataLocation(
      TableIdentifier tableId,
      String oldMetadataLocation,
      String newMetadataLocation);

  boolean deleteTable(TableIdentifier tableId);

  boolean renameTable(TableIdentifier oldTableId, TableIdentifier newTableId);

  default List<TableIdentifier> listTable(Namespace namespace) {
    throw new UnsupportedOperationException("List unsupported");
  };
}
