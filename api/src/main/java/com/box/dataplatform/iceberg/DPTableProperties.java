package com.box.dataplatform.iceberg;

public class DPTableProperties {
  private static final String CUSTOM_PREFIX = "dataplatform";

  private static String withPrefix(String s) {
    return String.format("%s.%s", CUSTOM_PREFIX, s);
  }

  public static final String MANIFEST_ENCRYPTED = withPrefix("manifest.encrypted");
  public static final boolean MANIFEST_ENCRYPTED_DEFAULT = false;

  public static final String WRITE_WITH_ENTITY_TRANSFORM =
      withPrefix("write.with-entity-transform");
  public static final String WRITE_WITH_ENTITY_TRANSFORM_DEFAULT = "false";

  public static final String ENCRYPTION_TYPE = withPrefix("manifest.encryptionType");
  public static final String ENCRYPTION_TYPE_DEFAULT = "kms";

  public static final String READ_DEDUPE_FILES = withPrefix("read.dedupe.files");
}
