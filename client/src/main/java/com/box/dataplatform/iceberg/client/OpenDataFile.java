package com.box.dataplatform.iceberg.client;

import com.box.dataplatform.iceberg.client.exceptions.DPIcebergRetryableException;
import com.box.dataplatform.iceberg.client.exceptions.RecordTransformationException;
import org.apache.iceberg.DataFile;

/** @param <RecordType> */
public interface OpenDataFile<RecordType> {

  void start() throws DPIcebergRetryableException;

  void add(RecordType record) throws DPIcebergRetryableException, RecordTransformationException;

  DataFile complete() throws DPIcebergRetryableException;
}
