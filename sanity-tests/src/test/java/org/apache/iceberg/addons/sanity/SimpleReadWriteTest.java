package org.apache.iceberg.addons.sanity;

import org.apache.iceberg.*;
import org.apache.iceberg.addons.mock.MockContext;
import org.apache.iceberg.addons.mock.MockContextId;
import org.apache.iceberg.addons.testkit.MockContextIdUtil;
import org.apache.iceberg.addons.testkit.sampletables.SimpleTableSpec;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleReadWriteTest {
  MockContextId contextId;
  TableIdentifier tableId;
  Table table;
  @Before
  public void before() {
    contextId = MockContextIdUtil.newContextId();
    tableId = SimpleTableSpec.INSTANCE.newId();
    table = SimpleTableSpec.INSTANCE.create(tableId, contextId);
  }

  @After
  public void after() {
    MockContext.clearContext(contextId);
  }
  
  @Test
  public void simpleReadWrite() throws IOException {
    table.refresh();
    Schema schema = table.schema();
    List<Record> expected = RandomGenericData.generate(schema, 100, 435691832918L);
    DataFile myFile = writeFile("myfile.avro", expected);
    AppendFiles appendFiles = table.newFastAppend();
    appendFiles.appendFile(myFile).commit();
    table.refresh();
    List<Record> result = Lists.newArrayList(IcebergGenerics.read(table).build());
    Assert.assertEquals(expected.size(), result.size());
  }

  private DataFile writeFile(String filename, List<Record> records) throws IOException {
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

    OutputFile outputFile = table.io().newOutputFile(
        table.locationProvider().newDataLocation(filename)
    );

    FileAppender<Record> fileAppender = new GenericAppenderFactory(table.schema()).newAppender(
        outputFile,
        fileFormat
    );

    try (FileAppender<Record> appender = fileAppender) {
      appender.addAll(records);
    }

    return DataFiles.builder(table.spec())
        .withInputFile(table.io().newInputFile(outputFile.location()))
        .withMetrics(fileAppender.metrics())
        .build();
  }
}
