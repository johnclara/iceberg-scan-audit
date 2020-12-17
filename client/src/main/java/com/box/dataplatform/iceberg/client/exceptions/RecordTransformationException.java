package com.box.dataplatform.iceberg.client.exceptions;

/** */
public class RecordTransformationException extends RuntimeException {
  public RecordTransformationException(Throwable t) {
    super("Record transformation failed.", t);
  }
}
