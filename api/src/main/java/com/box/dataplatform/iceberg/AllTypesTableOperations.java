package com.box.dataplatform.iceberg;

import com.box.dataplatform.iceberg.io.DPFileIO;
import com.box.dataplatform.iceberg.metastore.Metastore;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;

public class AllTypesTableOperations extends BaseMetastoreTableOperations {
  private final String tenantName;
  private final String icebergTableName;
  private final Metastore metastore;
  private final DPFileIO dpFileIO;

  protected AllTypesTableOperations(
      String tenantName, String icebergTableName, Metastore metastore, DPFileIO dpFileIO) {
    this.tenantName = tenantName;
    this.icebergTableName = icebergTableName;
    this.metastore = metastore;
    this.dpFileIO = dpFileIO;
  }

  @Override
  public DPFileIO io() {
    return dpFileIO;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    String originalMetadataFileLocation = super.metadataFileLocation(fileName);
    return io().addHintsForMetadataLocation(
            fileName,
            originalMetadataFileLocation,
            current()
                .propertyAsBoolean(
                    DPTableProperties.MANIFEST_ENCRYPTED,
                    DPTableProperties.MANIFEST_ENCRYPTED_DEFAULT));
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = metastore.getMetadataForTable(tenantName, icebergTableName);
    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String oldMetadataLocation = currentMetadataLocation();
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    metastore.updateMetadataLocation(
        tenantName, icebergTableName, oldMetadataLocation, newMetadataLocation);
  }
}
