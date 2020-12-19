package com.box.dataplatform.iceberg.io.mock;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.iceberg.io.PositionOutputStream;

public class WrappedPositionOutputStream extends PositionOutputStream {
  private final OutputStream outputStream;
  private long pos = 0L;

  public WrappedPositionOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
    pos += 1;
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
    outputStream.close();
  }
}
