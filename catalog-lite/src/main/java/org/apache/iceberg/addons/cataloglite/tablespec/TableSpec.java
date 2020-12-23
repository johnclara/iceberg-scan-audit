package org.apache.iceberg.addons.cataloglite.tablespec;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable and reusable builders to be used as test table templates.
 */
public class TableSpec implements Serializable {
  private final Schema schema;
  private final Lockable<PartitionSpec> partitionSpec;
  private final Lockable<HashMap<String, String>> properties;
  private final Lockable<SerializableTableIdentifier> tableId;

  public static TableSpec builder(Schema schema) {
    return new TableSpec(
        checkNotNull(schema),
        Lockable.of(null),
        Lockable.of(new HashMap<>()),
        Lockable.of(null)
    );
  }

  protected TableSpec(Schema schema,
                      Lockable<PartitionSpec> partitionSpec,
                      Lockable<HashMap<String, String>> properties,
                      Lockable<SerializableTableIdentifier> tableId) {
    this.schema = schema;
    this.partitionSpec = partitionSpec;
    this.properties = properties;
    this.tableId = tableId;
  }

  public PartitionSpec.Builder partitionSpecBuilder() {
    return PartitionSpec.builderFor(schema);
  }

  public TableSpec withPartitionSpec(Function<PartitionSpec.Builder, PartitionSpec> fromBuilder) {
    return new TableSpec(
        schema,
        partitionSpec.set(fromBuilder.apply(partitionSpecBuilder())),
        properties,
        tableId
    );
  }

  public TableSpec withPartitionSpec(PartitionSpec newPartitionSpec) {
    return new TableSpec(
        schema,
        partitionSpec.set(newPartitionSpec),
        properties,
        tableId
    );
  }

  public TableSpec withProperty(String key, String value) {
    HashMap<String, String> newProperties = new HashMap<>(properties.get());
    newProperties.put(checkNotNull(key), value);
    return new TableSpec(
        schema,
        partitionSpec,
        properties.set(newProperties),
        tableId
    );
  }

  public TableSpec withProperties(Map<String, String> additionalProperties) {
    HashMap<String, String> newProperties = new HashMap<>(properties.get());
    if (additionalProperties != null) {
      newProperties.putAll(additionalProperties);
    }
    return new TableSpec(
        schema,
        partitionSpec,
        properties.set(newProperties),
        tableId
    );
  }

  public TableSpec withTableId(TableIdentifier newTableId) {
    return new TableSpec(
        schema,
        partitionSpec,
        properties,
        tableId.set(SerializableTableIdentifier.of(checkNotNull(newTableId)))
    );
  }

  public TableSpec lockPartitionSpec() {
    return new TableSpec(
        schema,
        partitionSpec.lock(),
        properties,
        tableId
    );
  }

  public TableSpec lockProperties() {
    return new TableSpec(
        schema,
        partitionSpec,
        properties.lock(),
        tableId
    );
  }

  public Table create(Catalog catalog) {
    return checkNotNull(catalog).createTable(
        checkNotNull(tableId.get()).get(),
        schema,
        partitionSpec.get(),
        properties.get()
    );
  }

  public Schema schema() {
    return schema;
  }

  public Lockable<PartitionSpec> partitionSpec() {
    return partitionSpec;
  }

  public Lockable<HashMap<String, String>> properties() {
    return properties;
  }
}
