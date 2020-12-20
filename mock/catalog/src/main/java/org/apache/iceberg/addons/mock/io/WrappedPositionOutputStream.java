package org.apache.iceberg.addons.mock.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.iceberg.io.PositionOutputStream;

public class WrappedPositionOutputStream extends PositionOutputStream implements Serializable {
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
