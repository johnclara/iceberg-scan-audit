package org.apache.iceberg.addons.testkit.sampletables;

import org.apache.iceberg.Schema;
import org.apache.iceberg.addons.cataloglite.tablespec.TableSpec;
import org.apache.iceberg.addons.testkit.SampleTableSpec;
import org.apache.iceberg.types.Types;

import java.util.Collections;

import static org.apache.iceberg.types.Types.NestedField.required;

/** Schema with a single integer field: "myField" */
public class SimpleTableSpec extends SampleTableSpec {
  public static SimpleTableSpec INSTANCE = new SimpleTableSpec();

  private SimpleTableSpec() { }

  @Override
  protected String namePrefix() {
    return "simple";
  }

  @Override
  public TableSpec spec() {
    return TableSpec
          .builder(new Schema(
                required(0, "myField", Types.IntegerType.get())
          ))
          .withPartitionSpec((builder) -> builder.build())
          .lockPartitionSpec()
          .withProperties(Collections.emptyMap())
          .lockProperties();
  }
}
