package com.box.dataplatform.iceberg.client;

import com.box.dataplatform.iceberg.client.model.PartitionInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;

/** @param <RecordType> */
public class MultiTableDPIcebergClientImpl<RecordType>
    implements MultiTableDPIcebergClient<RecordType> {
  private final Map<String, DPIcebergClientImpl<RecordType>> clients;

  public MultiTableDPIcebergClientImpl(
      List<String> tableNames, Function<String, DPIcebergClientImpl<RecordType>> clientBuilder) {
    clients = new HashMap<>();
    for (String tableName : tableNames) {
      clients.put(tableName, clientBuilder.apply(tableName));
    }
  }

  private DPIcebergClientImpl<RecordType> getClient(String tableName) {
    DPIcebergClientImpl<RecordType> client = clients.get(tableName);
    if (client == null) {
      throw new IllegalArgumentException(
          "TableName: "
              + tableName
              + " is not one of the given tables: "
              + String.join(",", clients.keySet()));
    }
    return client;
  }

  public OpenDataFile<RecordType> createOpenDataFile(String tableName, List<RecordType> records) {
    DPIcebergClientImpl<RecordType> client = getClient(tableName);
    return client.createOpenDataFile(records);
  }

  public DataFile processBatch(String tableName, List<RecordType> records) {
    DPIcebergClientImpl<RecordType> client = getClient(tableName);
    return client.processBatch(records);
  }

  public PartitionInfo getPartitionForRecord(String tableName, RecordType record) {
    DPIcebergClientImpl<RecordType> client = getClient(tableName);
    return client.getPartitionForRecord(record);
  }

  public void commitBatch(String tableName, List<DataFile> datafiles) {
    DPIcebergClientImpl<RecordType> client = getClient(tableName);
    client.commitBatch(datafiles);
  }
}
