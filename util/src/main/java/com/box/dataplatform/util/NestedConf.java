package com.box.dataplatform.util;

public class NestedConf extends Conf {
  private final String namespace;
  private final Conf conf;

  protected NestedConf(Conf conf, String namespace) {
    super(namespace);
    this.namespace = namespace;
    this.conf = conf;
  }

  public static NestedConf of(Conf conf, String suffix) {
    return new NestedConf(conf, suffix + ".");
  }

  @Override
  protected String get(String key) {
    return conf.propertyAsString(key, null);
  }

  @Override
  protected void set(String key, String value) {
    conf.setString(key, value);
  }
}
