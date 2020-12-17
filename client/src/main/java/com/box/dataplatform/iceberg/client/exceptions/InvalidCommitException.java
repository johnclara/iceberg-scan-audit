package com.box.dataplatform.iceberg.client.exceptions;

/** */
public class InvalidCommitException extends IllegalArgumentException {
  public InvalidCommitException(Throwable lastThrowable) {
    super("Will not be able to commit this batch.", lastThrowable);
  }
}
