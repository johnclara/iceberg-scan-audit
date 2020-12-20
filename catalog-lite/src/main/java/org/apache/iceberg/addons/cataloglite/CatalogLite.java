package org.apache.iceberg.addons.cataloglite;

import java.util.List;

import org.apache.iceberg.addons.cataloglite.io.ObjectStore;
import org.apache.iceberg.addons.cataloglite.metastore.Metastore;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A less extensible catalog, in order to prune down the complexity
 */
public class CatalogLite extends BaseMetastoreCatalog {
  private static final Logger log = LoggerFactory.getLogger(CatalogLite.class);
  private final Metastore metastore;
  private final ObjectStore objectStore;

  public CatalogLite(Metastore metastore, ObjectStore objectStore) {
    this.metastore = metastore;
    this.objectStore = objectStore;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableId) {
    return new TableOperationsLite(tableId, metastore, objectStore);
  }

  /**
   * Will not actually purge
   *
   * @param identifier
   * @param purge
   * @return
   */
  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return metastore.deleteTable(identifier);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    metastore.renameTable(from, to);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableId) {
    return String.format("/%s/%s/", tableId.namespace(), tableId.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    throw new UnsupportedOperationException("Metastore store will not list.");
  }

  @Override
  protected String name() {
    return "lite";
  }
}
