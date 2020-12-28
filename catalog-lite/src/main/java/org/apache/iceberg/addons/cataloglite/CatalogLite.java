package org.apache.iceberg.addons.cataloglite;

import java.util.List;
import java.util.Map;

import org.apache.iceberg.addons.cataloglite.io.ObjectStore;
import org.apache.iceberg.addons.cataloglite.metastore.Metastore;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new LiteMetastoreCatalogTableBuilder(identifier, schema);
  }

  protected class LiteMetastoreCatalogTableBuilder implements TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    public LiteMetastoreCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      Preconditions.checkArgument(isValidIdentifier(identifier), "Invalid table identifier: %s", identifier);

      this.identifier = identifier;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        propertiesBuilder.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, properties);

      try {
        ops.commit(null, metadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Table was created concurrently: %s", identifier);
      }

      return new AuditedBaseTable(ops, fullTableName(name(), identifier));
    }

    @Override
    public Transaction createTransaction() {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      Map<String, String> properties = propertiesBuilder.build();
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, properties);
      return Transactions.createTableTransaction(identifier.toString(), ops, metadata);
    }

    @Override
    public Transaction replaceTransaction() {
      return newReplaceTableTransaction(false);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      return newReplaceTableTransaction(true);
    }

    private Transaction newReplaceTableTransaction(boolean orCreate) {
      TableOperations ops = newTableOps(identifier);
      if (!orCreate && ops.current() == null) {
        throw new NoSuchTableException("No such table: %s", identifier);
      }

      TableMetadata metadata;
      if (ops.current() != null) {
        String baseLocation = location != null ? location : ops.current().location();
        metadata = ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, propertiesBuilder.build());
      } else {
        String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
        metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, propertiesBuilder.build());
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(identifier.toString(), ops, metadata);
      } else {
        return Transactions.replaceTableTransaction(identifier.toString(), ops, metadata);
      }
    }
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    if (isValidIdentifier(identifier)) {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        Table result = new AuditedBaseTable(ops, fullTableName(name(), identifier));
        log.info("Table loaded by catalog: {}", result);
        return result;
      }
    }
    return super.loadTable(identifier);
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
  public String name() {
    return "lite";
  }
}
