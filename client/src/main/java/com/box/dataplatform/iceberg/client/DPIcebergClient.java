package com.box.dataplatform.iceberg.client;

import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import com.box.dataplatform.iceberg.client.exceptions.InvalidCommitException;
import com.box.dataplatform.iceberg.client.exceptions.RecordTransformationException;
import com.box.dataplatform.iceberg.client.model.PartitionInfo;
import java.util.List;
import org.apache.iceberg.DataFile;

/** @param <RecordType> */
public interface DPIcebergClient<RecordType> {

  OpenDataFile<RecordType> createOpenDataFile(List<RecordType> firstRecord)
      throws DPIcebergRetryableException;

  PartitionInfo getPartitionForRecord(RecordType record);

  DataFile processBatch(List<RecordType> records)
      throws DPIcebergRetryableException, RecordTransformationException;

  void commitBatch(List<DataFile> datafiles)
      throws DPIcebergRetryableException, InvalidCommitException;
}
