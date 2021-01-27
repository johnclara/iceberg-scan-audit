package org.apache.iceberg.addons.sanity;

import org.apache.iceberg.*;
import org.apache.iceberg.addons.mock.MockCatalog;
import org.apache.iceberg.addons.mock.MockContext;
import org.apache.iceberg.addons.mock.MockContextId;
import org.apache.iceberg.addons.scanaudit.ScanAuditEvaluateEvent;
import org.apache.iceberg.addons.scanaudit.ScanAuditEventHandler;
import org.apache.iceberg.addons.scanaudit.ScanAuditTableSchema;
import org.apache.iceberg.addons.testkit.MockContextIdUtil;
import org.apache.iceberg.addons.testkit.TableIdentifierUtil;
import org.apache.iceberg.addons.testkit.sampletables.SimpleTableSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;

public class ScanListenerTest {
  MockContextId contextId;
  TableIdentifier tableId;
  Table table;
  Table auditTable;

  TableIdentifier auditTableId;
  ScanAuditEventHandler listener;
  @Before
  public void before() {
    contextId = MockContextIdUtil.newContextId();
    tableId = SimpleTableSpec.INSTANCE.newId();
    Catalog catalog = new MockCatalog(contextId);
    table = SimpleTableSpec.INSTANCE.create(tableId, catalog);


    auditTableId = TableIdentifierUtil.newTableId("audit");
    auditTable = catalog.createTable(auditTableId, ScanAuditTableSchema.INSTANCE);
    auditTable = catalog.loadTable(auditTableId);
    auditTable.updateProperties().set(DEFAULT_FILE_FORMAT, "avro").commit();
    listener = new ScanAuditEventHandler(auditTable);
  }

  @After
  public void after() {
    MockContext.clearContext(contextId);
  }
  
  @Test
  public void simpleReadWrite() throws IOException {
    commit(table);
    table.refresh();


    listener.register();

    CloseableIterable<Record> reader = IcebergGenerics.read(table).build();
    List<Record> result = Lists.newArrayList(reader);

    listener.close();

    auditTable.refresh();
    CloseableIterable<Record> auditReader = IcebergGenerics.read(auditTable).build();
    List<Record> auditResult = Lists.newArrayList(auditReader);
  }

  private void commit(Table table) throws IOException {
    List<Record> expected = RandomGenericData.generate(table.schema(), 100, 435691832918L);
    DataFile myFile = writeFile("myfile.avro", expected);
    table.refresh();
    AppendFiles appendFiles = table.newFastAppend();
    appendFiles.appendFile(myFile).commit();
  }


  private DataFile writeFile(String filename, List<Record> records) throws IOException {
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

    OutputFile outputFile = table.io().newOutputFile(
        table.locationProvider().newDataLocation(filename)
    );

    FileAppender<Record> fileAppender = new GenericAppenderFactory(table.schema())
        .newAppender(
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
