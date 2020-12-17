package com.box.dataplatform.iceberg.metastore;

import java.io.Serializable;

public interface Metastore extends Serializable {
  String getMetadataForTable(String tenantName, String icebergTableName);

  void updateMetadataLocation(
      String tenantName,
      String icebergTableName,
      String oldMetadataLocation,
      String newMetadataLocation);

  boolean deleteTable(String tenantName, String icebergTableName);

  boolean renameTable(String tenantName, String from, String to);
}
