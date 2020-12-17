package com.box.dataplatform.iceberg.client.exceptions;

/** exceptions from DSE client which can be retried by DSE instead of failing batch. */
public class DPIcebergRetryableException extends RuntimeException {
  public DPIcebergRetryableException(String errorMessage, Throwable err) {
    super(errorMessage, err);
  }
}
