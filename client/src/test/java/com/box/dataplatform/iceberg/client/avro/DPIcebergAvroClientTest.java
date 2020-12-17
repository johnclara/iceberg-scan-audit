package com.box.dataplatform.iceberg.client.avro;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.Test;

/** unit tests for {@link DPIcebergAvroClient} */
public class DPIcebergAvroClientTest {

  /*
  @Test
  public void testProcessBatchRetryException() {
      TableIdentifier ti = TableIdentifier.of("a", "b");
      BaseTable t = mock(BaseTable.class);
      PartitionSpec spec = mock(PartitionSpec.class);
      when(t.spec()).thenReturn(spec);
      List<PartitionField> fields = new ArrayList<>();
      when(spec.fields()).thenReturn(fields);
      doThrow(new AmazonClientException("test")).when(t).refresh();

      DPIcebergAvroClient client = new DPIcebergAvroClient(ti, t, 1, false);
      try {
          List<GenericRecord> lst = new java.util.ArrayList<>();
          lst.add(null);
          client.processBatch(lst);
      } catch (Exception ex) {
          assertTrue("got DPIcebergRetryableException", ex instanceof DPIcebergRetryableException);
          assertEquals("failed to refresh table: test", ex.getMessage());
      }
  }
   */

  @Test
  public void testCommitBatchRetryException() {
    TableIdentifier ti = TableIdentifier.of("a", "b");
    BaseTable t = mock(BaseTable.class);
    PartitionSpec spec = mock(PartitionSpec.class);
    when(t.spec()).thenReturn(spec);
    List<PartitionField> fields = new ArrayList<>();
    when(spec.fields()).thenReturn(fields);
    AppendFiles appendFiles = mock(AppendFiles.class);
    when(t.newAppend()).thenReturn(appendFiles);
    when(appendFiles.appendFile(any())).thenReturn(null);
    doThrow(new CommitFailedException("test")).when(appendFiles).commit();

    DPIcebergAvroClient client = new DPIcebergAvroClient(ti, t, 1, false);
    try {
      client.commitBatch(new ArrayList<>());
    } catch (Exception ex) {
      assertTrue("got DPIcebergRetryableException", ex instanceof DPIcebergRetryableException);
      assertEquals("failed commit.", ex.getMessage());
    }
  }
}
