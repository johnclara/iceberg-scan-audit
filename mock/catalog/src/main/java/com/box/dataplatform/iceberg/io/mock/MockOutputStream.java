package com.box.dataplatform.iceberg.io.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class MockOutputStream extends ByteArrayOutputStream {
  private final AtomicReference<MockOutputStream> ref;
  private final MockOutputStream oldValue;
  public MockOutputStream(AtomicReference<MockOutputStream> ref, MockOutputStream oldValue) {
    this.ref = ref;
    this.oldValue = oldValue;
  }


  @Override
  public void close() throws IOException {
    super.close();
    ref.compareAndSet(oldValue, this);
  }
}
