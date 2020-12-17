package com.box.dataplatform.iceberg.util;

import org.apache.iceberg.common.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtil.class);

  public static <T> T initializeNoArgConstructor(String impl) {
    LOG.info("Loading custom implementation: {}", impl);
    DynConstructors.Ctor<T> ctor;
    try {
      ctor = DynConstructors.builder().impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize missing no-arg constructor: %s", impl), e);
    }

    T obj;
    try {
      obj = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize %s.", impl), e);
    }
    return obj;
  }
}
