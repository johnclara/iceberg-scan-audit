package org.apache.iceberg.addons.mock.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

public class MockOutputStream extends ByteArrayOutputStream implements Serializable {
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
