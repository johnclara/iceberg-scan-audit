package com.box.dataplatform.iceberg.core.testkit.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestkitUtils {
  public static final Logger logger = LoggerFactory.getLogger(TestkitUtils.class);

  public static String getAndCheckProperty(String systemProperty) {
    String propertyValue = System.getProperty(systemProperty);
    if (propertyValue == null) {
      throw new IllegalArgumentException("sqlite system property not set");
    } else {
      logger.info("Using sqlite path " + systemProperty + "=" + propertyValue);
    }
    return propertyValue;
  }
}
