package org.apache.iceberg.addons.io.mock;

import org.apache.iceberg.addons.io.FileObject;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicReference;

public class MockFileObject implements FileObject {
  private final String location;
  private final AtomicReference<MockOutputStream> ref;

  public MockFileObject(String location) {
    this.location = location;
    this.ref = new AtomicReference<>();
  }

  @Override
  public long getLength() {
    MockOutputStream mockOutputStream = ref.get();
    if (mockOutputStream == null) {
      throw new NotFoundException("File at {} not created yet.", location);
    } else {
      return mockOutputStream.size();
    }
  }

  @Override
  public SeekableInputStream newStream() {
    MockOutputStream mockOutputStream = ref.get();
    if (mockOutputStream == null) {
      throw new NotFoundException("File at {} not created yet.", location);
    } else {
      return new WrappedSeekableInputStream(() -> new ByteArrayInputStream(mockOutputStream.toByteArray()));
    }
  }

  @Override
  public PositionOutputStream create() {
    MockOutputStream oldVersion = ref.get();
    if (oldVersion != null) {
      throw new AlreadyExistsException("File at {} already exists.", location);
    } else {
      return createOrOverwrite(oldVersion);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    MockOutputStream oldVersion = ref.get();
    return createOrOverwrite(oldVersion);
  }

  private PositionOutputStream createOrOverwrite(MockOutputStream oldVersion) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    MockOutputStream newOutputStream = new MockOutputStream(ref, oldVersion);
    return new WrappedPositionOutputStream(newOutputStream);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public InputFile toInputFile() {
    return this;
  }

  @Override
  public boolean exists() {
    return ref.get() != null;
  }
}
