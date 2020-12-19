package com.box.dataplatform.iceberg.io;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.Serializable;

public interface FileObject extends InputFile, OutputFile, Serializable {
  @Override
  long getLength();

  @Override
  SeekableInputStream newStream();

  @Override
  PositionOutputStream create();

  @Override
  PositionOutputStream createOrOverwrite();

  @Override
  String location();

  @Override
  InputFile toInputFile();

  @Override
  boolean exists();
}
