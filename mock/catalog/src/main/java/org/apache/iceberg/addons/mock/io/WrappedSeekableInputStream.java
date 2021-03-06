package org.apache.iceberg.addons.mock.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.function.Supplier;
import org.apache.iceberg.io.SeekableInputStream;

public class WrappedSeekableInputStream extends SeekableInputStream implements Serializable {
  private final Supplier<InputStream> iss;

  private long pos = 0L;
  private InputStream rawStream;

  public WrappedSeekableInputStream(Supplier<InputStream> iss) {
    this.iss = iss;
    rawStream = iss.get();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long newPos) throws IOException {
    rawStream = iss.get();
    rawStream.skip(newPos);
    pos = newPos;
  }

  @Override
  public int read() throws IOException {
    int numRead = rawStream.read();
    if (numRead > 0) {
      pos += numRead;
    }
    return numRead;
  }
}

