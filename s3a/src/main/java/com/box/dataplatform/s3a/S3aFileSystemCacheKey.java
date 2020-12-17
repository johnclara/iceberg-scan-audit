package com.box.dataplatform.s3a;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** This is all hard coded instead of passing around config. */
public class S3aFileSystemCacheKey {
  private static final Map<String, S3aFileSystemCacheKey> schemeToConfig = new HashMap<>();
  private static final Map<S3aFileSystemCacheKey, String> configToScheme = new HashMap<>();

  private static void addScheme(String scheme, S3aFileSystemCacheKey config) {
    schemeToConfig.put(scheme, config);
    configToScheme.put(config, scheme);
  }

  static {
    addScheme("dpv1", new S3aFileSystemCacheKey(true, true, true));
    addScheme("meta", new S3aFileSystemCacheKey(true, true, false));
    addScheme("s3a", new S3aFileSystemCacheKey(true, false, false));
    addScheme("rometa", new S3aFileSystemCacheKey(false, true, false));
    addScheme("ros3a", new S3aFileSystemCacheKey(false, false, false));
  };

  private final boolean writeAccessToBucket;
  private final boolean clientSideEncrypted;
  private final boolean useInstructionFile;

  public String toScheme() {
    return configToScheme.get(this);
  }

  public static S3aFileSystemCacheKey fromScheme(String scheme) {
    return schemeToConfig.get(scheme);
  }

  public static Set<String> allSchemes() {
    return schemeToConfig.keySet();
  }

  public S3aFileSystemCacheKey(
      boolean writeAccessToBucket, boolean clientSideEncrypted, boolean useInstructionFile) {
    this.writeAccessToBucket = writeAccessToBucket;
    this.clientSideEncrypted = clientSideEncrypted;
    this.useInstructionFile = useInstructionFile;
  }

  public boolean writeAccessToBucket() {
    return writeAccessToBucket;
  }

  public boolean clientSideEncrypted() {
    return clientSideEncrypted;
  }

  public boolean useInstructionFile() {
    return useInstructionFile;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof S3aFileSystemCacheKey)) return false;
    S3aFileSystemCacheKey that = (S3aFileSystemCacheKey) o;
    return writeAccessToBucket == that.writeAccessToBucket
        && clientSideEncrypted == that.clientSideEncrypted
        && useInstructionFile == that.useInstructionFile;
  }

  @Override
  public int hashCode() {
    return Objects.hash(writeAccessToBucket, clientSideEncrypted, useInstructionFile);
  }
}
