package com.box.dataplatform.util;

import java.util.Map;

public class Properties extends Conf {
  private final Map<String, String> properties;

  protected Properties(Map<String, String> properties) {
    super();
    this.properties = properties;
  }

  protected Properties(String namespace, Map<String, String> properties) {
    super(namespace);
    this.properties = properties;
  }

  public static Properties of(String namespace, Map<String, String> properties) {
    return new Properties(namespace, properties);
  }

  public static Properties of(Map<String, String> properties) {
    return new Properties(properties);
  }

  @Override
  protected String get(String key) {
    return properties.get(key);
  }

  @Override
  protected void set(String key, String value) {
    properties.put(key, value);
  }
}
