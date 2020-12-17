package com.box.dataplatform.iceberg.client.exceptions;

/** An exception for a non optional union field */
public class NonOptionUnionException extends IllegalArgumentException {
  public NonOptionUnionException() {
    super("The schema does not support non optional union types");
  }
}
