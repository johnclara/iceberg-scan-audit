package com.box.dataplatform.iceberg.client;

import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import com.box.dataplatform.iceberg.client.exceptions.InvalidCommitException;
import com.box.dataplatform.iceberg.client.model.PartitionInfo;
import java.util.List;
import org.apache.iceberg.DataFile;

/** @param <RecordType> */
public interface MultiTableDPIcebergClient<RecordType> {

  OpenDataFile<RecordType> createOpenDataFile(String tableName, List<RecordType> records)
      throws DPIcebergRetryableException;

  DataFile processBatch(String tableName, List<RecordType> records)
      throws DPIcebergRetryableException;

  PartitionInfo getPartitionForRecord(String tableName, RecordType record);

  void commitBatch(String tableName, List<DataFile> datafiles)
      throws DPIcebergRetryableException, InvalidCommitException;
}
