package org.apache.iceberg.addons.cataloglite.tablespec;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.Serializable;

public class SerializableTableIdentifier implements Serializable {
  private final String namespace;
  private final String name;

  public static SerializableTableIdentifier of(TableIdentifier tableId) {
    return new SerializableTableIdentifier(tableId.namespace().toString(), tableId.name());
  }
  private SerializableTableIdentifier(String namespace, String name) {
    this.namespace = namespace;
    this.name = name;
  }

  public TableIdentifier get() {
    return TableIdentifier.of(Namespace.of(namespace), name);
  }
}
