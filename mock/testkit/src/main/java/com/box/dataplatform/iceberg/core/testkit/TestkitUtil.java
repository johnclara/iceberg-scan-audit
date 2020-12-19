package com.box.dataplatform.iceberg.core.testkit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class TestkitUtil {
  public static final Logger logger = LoggerFactory.getLogger(TestkitUtil.class);

  /* TODO remove
  public static String getAndCheckProperty(String systemProperty) {
    String propertyValue = System.getProperty(systemProperty);
    if (propertyValue == null) {
      throw new IllegalArgumentException("sqlite system property not set");
    } else {
      logger.info("Using sqlite path " + systemProperty + "=" + propertyValue);
    }
    return propertyValue;
  }
   */

  public static String uniquify(String prefix) {
    String uuid = UUID.randomUUID().toString().replaceAll("-", "_");
    return String.format("{}_{}", prefix, uuid);
  }
}
