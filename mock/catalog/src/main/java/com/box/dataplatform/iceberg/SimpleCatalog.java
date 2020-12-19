package com.box.dataplatform.iceberg;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.box.dataplatform.iceberg.mock.MockContext;
import com.box.dataplatform.iceberg.mock.MockContextKey;
import com.box.dataplatform.iceberg.io.mock.MockObjectStore;
import com.box.dataplatform.iceberg.io.ObjectStore;
import com.box.dataplatform.iceberg.metastore.Metastore;
import com.box.dataplatform.iceberg.metastore.mock.MockMetastore;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleCatalog extends BaseMetastoreCatalog {
  private static final ConcurrentHashMap<MockContextKey, MockContext> mockContexts = new ConcurrentHashMap<>();
  public static boolean clearContext(String context) {
    return mockContexts.remove(context) != null;
  }

  private static final Logger log = LoggerFactory.getLogger(SimpleCatalog.class);
  private final Metastore metastore;
  private final ObjectStore objectStore;

  public SimpleCatalog(Metastore metastore, ObjectStore objectStore) {
    this.metastore = metastore;
    this.objectStore = objectStore;
  }

  public SimpleCatalog(MockContextKey contextKey) {
    this(mockContexts.computeIfAbsent(contextKey, (k) -> new MockContext(k, new MockObjectStore(k), new MockMetastore(k))));
  }

  protected SimpleCatalog(MockContext context) {
    this(context.metastore(), context.objectStore());
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableId) {
    return new BasicTableOperations(tableId, metastore, objectStore);
  }

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
    return String.format("/{}/{}/", tableId.namespace(), tableId.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    throw new UnsupportedOperationException("Metastore store will not list.");
  }

  @Override
  protected String name() {
    return "simple";
  }
}