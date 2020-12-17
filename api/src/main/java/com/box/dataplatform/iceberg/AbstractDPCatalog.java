package com.box.dataplatform.iceberg;

import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.iceberg.encryption.crypto.SymmetricKeyCryptoFormat;
import com.box.dataplatform.iceberg.encryption.dekprovider.DelegatingDekProvider;
import com.box.dataplatform.iceberg.io.DPFileIO;
import com.box.dataplatform.iceberg.metastore.Metastore;
import com.box.dataplatform.iceberg.util.MapSerde;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDPCatalog extends BaseMetastoreCatalog {
  private static final Logger log = LoggerFactory.getLogger(AbstractDPCatalog.class);

  private static final Joiner DOT = Joiner.on('.');

  private final Metastore metastore;
  private final DPFileIO dpFileIO;
  private final DelegatingDekProvider delegatingDekProvider;
  private final DelegatingDekProvider.DelegateKekId<? extends KekId> encryptedKekId;
  private final DelegatingDekProvider.DelegateKekId<? extends KekId> plaintextKekId;
  private final SymmetricKeyCryptoFormat cryptoFormat;
  private final MapSerde serde;

  public AbstractDPCatalog(
      Metastore metastore,
      DPFileIO dpFileIO,
      DelegatingDekProvider delegatingDekProvider,
      DelegatingDekProvider.DelegateKekId<? extends KekId> encryptedKekId,
      DelegatingDekProvider.DelegateKekId<? extends KekId> plaintextKekId,
      SymmetricKeyCryptoFormat cryptoFormat,
      MapSerde serde) {
    this.metastore = metastore;
    this.dpFileIO = dpFileIO;
    this.delegatingDekProvider = delegatingDekProvider;
    this.encryptedKekId = encryptedKekId;
    this.plaintextKekId = plaintextKekId;
    this.cryptoFormat = cryptoFormat;
    this.serde = serde;
  }

  private String getTenantName(TableIdentifier tableIdentifier) {
    return tableIdentifier.namespace().level(0);
  }

  private String getTableName(TableIdentifier tableIdentifier) {
    List<String> levels = new LinkedList<>(Arrays.asList(tableIdentifier.namespace().levels()));
    levels.add(tableIdentifier.name());
    levels.remove(0);
    return DOT.join(levels);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String tenantName = getTenantName(tableIdentifier);
    String tableName = getTableName(tableIdentifier);
    // instantiate the DynamoDB table operations.
    if (MetadataTableType.from(tableIdentifier.name()) != null) {
      return new AllTypesTableOperations(tenantName, tableName, metastore, dpFileIO);
    } else {
      return new DataTableOperations<>(
          tenantName,
          tableName,
          metastore,
          dpFileIO,
          cryptoFormat,
          serde,
          delegatingDekProvider,
          encryptedKekId,
          plaintextKekId);
    }
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    throw new IllegalArgumentException(
        "Attempting to generate a default table location. All table locations must be explicitly provided on table creation");
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    throw new UnsupportedOperationException("List tables has not been implemented yet");
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String tenantName = getTenantName(identifier);
    String tableName = getTableName(identifier);
    try {
      TableOperations ops = newTableOps(identifier);
      TableMetadata lastMetadata;
      if (purge && ops.current() != null) {
        lastMetadata = ops.current();
      } else {
        lastMetadata = null;
      }
      boolean deleted = metastore.deleteTable(tenantName, tableName);
      if (deleted && purge && lastMetadata != null) {
        long start = System.currentTimeMillis();
        BaseMetastoreCatalog.dropTableData(ops.io(), lastMetadata);
        long end = System.currentTimeMillis();
        log.warn(
            "dropTableData for tenant: {} table: {} took: {} seconds",
            tenantName,
            tableName,
            (start - end) / 1000);
      }
      return true;
    } catch (Exception e) {
      throw new RuntimeException("Failed to drop " + identifier.toString(), e);
    }
  }

  /**
   * By default loadMetadataTable will create table ops for the data table and use them as ops.
   * However, this includes an EncryptionManager which wraps the table's io during tasks. We
   * override this to not create a default ops for the data table, but instead create an ops without
   * the encryption manager.
   *
   * @param identifier
   * @return
   */
  Table loadMetadataTable(TableIdentifier identifier) {
    String name = identifier.name();
    MetadataTableType type = MetadataTableType.from(name);
    if (type != null) {
      TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      String tenantName = getTenantName(baseTableIdentifier);
      String tableName = getTableName(baseTableIdentifier);
      TableOperations ops = new AllTypesTableOperations(tenantName, tableName, metastore, dpFileIO);

      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: " + baseTableIdentifier);
      }

      Table baseTable = new BaseTable(ops, fullTableName(name(), baseTableIdentifier));

      switch (type) {
        case ENTRIES:
          return new ManifestEntriesTable(ops, baseTable);
        case FILES:
          return new DataFilesTable(ops, baseTable);
        case HISTORY:
          return new HistoryTable(ops, baseTable);
        case SNAPSHOTS:
          return new SnapshotsTable(ops, baseTable);
        case MANIFESTS:
          return new ManifestsTable(ops, baseTable);
        case PARTITIONS:
          return new PartitionsTable(ops, baseTable);
        case ALL_DATA_FILES:
          return new AllDataFilesTable(ops, baseTable);
        case ALL_MANIFESTS:
          return new AllManifestsTable(ops, baseTable);
        case ALL_ENTRIES:
          return new AllEntriesTable(ops, baseTable);
        default:
          throw new NoSuchTableException(
              "Unknown metadata table type: %s for %s", type, baseTableIdentifier);
      }

    } else {
      throw new NoSuchTableException("Table does not exist: " + identifier);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    String fromTenantName = getTenantName(from);
    String toTenantName = getTenantName(to);
    if (!fromTenantName.equals(toTenantName)) {
      throw new IllegalArgumentException("Cannot move table between tenants");
    }
    // Example service to  table
    metastore.renameTable(getTableName(from), fromTenantName, toTenantName);
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    if (isValidIdentifier(identifier)) {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() == null) {
        if (MetadataTableType.from(identifier.name()) != null) {
          return loadMetadataTable(identifier);
        }
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      } else {
        return new DPBaseTable(ops, fullTableName(name(), identifier));
      }
    } else {
      return super.loadTable(identifier);
    }
  }

  @Override
  protected String name() {
    return "dynamodb";
  }
}
