package com.box.dataplatform.iceberg.io;

import java.util.Map;
import org.apache.iceberg.io.FileIO;

public interface DPFileIO extends FileIO {
  String addHintsForMetadataLocation(
      String fileName, String originalFileLocation, boolean encryptManifest);

  void initialize(Map<String, String> options);
}
