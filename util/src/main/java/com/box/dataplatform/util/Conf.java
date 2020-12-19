package com.box.dataplatform.util;

import java.util.function.Function;

/**
 * Don't worry! This is basically just a wrapper around Map<String, String>.
 *
 * <p>It adds a namespace to all the properties so that none of our properties collide with
 * anything.
 *
 * <p>The reason you can't see the underlying map is to also support hadoop configuration {@link
 * com.box.dataplatform.s3a.HadoopConf}
 *
 * <p>Once we migrate off of hadoopconf entirely we can switch over to just properties which will be
 * a true wrapper around Map<String, String>
 *
 * <p>namespace should not be used as a tool for flattening nested structures. (see legacy analytics
 * conf for how confusing that can get)
 *
 * <p>This is based on {@link org.apache.iceberg.util.PropertyUtil}
 */
public abstract class Conf {
  public static final String DEFAULT_NAMESPACE = "box.dp.";

  private final String namespace;

  protected abstract String get(String key);

  protected abstract void set(String key, String value);

  protected Conf() {
    namespace = DEFAULT_NAMESPACE;
  }

  protected Conf(String namespace) {
    this.namespace = namespace;
  }

  public String withNamespace(String property) {
    return namespace + property;
  }

  private <V> V propertyAs(String property, V defaultValue, Function<String, V> parser) {
    return propertyAs(property, defaultValue, parser, false);
  }
  /**
   * When conf passes through spark, some values are lowercased. But the json encoding for the Dek &
   * kekId is cased. Conf is used for reading the kekId from spark options and persisting it to the
   * KeyMetadata byte buffer. This will try reading both types, but only write cased.
   */
  private <V> V propertyAs(
      String property, V defaultValue, Function<String, V> parser, Boolean checkLowercase) {
    String namespacedProperty = withNamespace(property);
    String value = get(namespacedProperty);
    if (value != null) {
      return parser.apply(value);
    } else if (checkLowercase) {
      return propertyAs(property.toLowerCase(), defaultValue, parser, false);
    }
    return defaultValue;
  }

  private <V> V propertyAs(String property, Function<String, V> parser) {
    return propertyAs(property, parser, false);
  }

  private <V> V propertyAs(String property, Function<String, V> parser, Boolean checkLowercase) {
    String namespacedProperty = withNamespace(property);
    String value = get(namespacedProperty);
    if (value != null) {
      return parser.apply(value);
    } else if (checkLowercase) {
      return propertyAs(property.toLowerCase(), parser, false);
    } else {
      throw new IllegalArgumentException(
          String.format("Property %s required ", namespacedProperty));
    }
  }

  private <V> void set(String property, V value, Function<V, String> asString) {
    if (value == null) {
      throw new IllegalArgumentException(
          String.format("Can't set null property property %s ", property));
    }
    String namespacedProperty = withNamespace(property);
    String valueAsString = asString.apply(value);
    set(namespacedProperty, valueAsString);
  }

  public boolean containsKey(String property) {
    return propertyAsString(property, null) != null;
  }

  public void setBoolean(String property, boolean value) {
    set(property, value, String::valueOf);
  }

  public void setDouble(String property, double value) {
    set(property, value, String::valueOf);
  }

  public void setInt(String property, int value) {
    set(property, value, String::valueOf);
  }

  public void setLong(String property, long value) {
    set(property, value, String::valueOf);
  }

  public void setString(String property, String value) {
    set(property, value, Function.identity());
  }

  public boolean propertyAsBoolean(String property) {
    return propertyAs(property, Boolean::parseBoolean);
  }

  public boolean propertyAsBoolean(String property, boolean defaultValue) {
    return propertyAs(property, defaultValue, Boolean::parseBoolean);
  }

  public double propertyAsDouble(String property) {
    return propertyAs(property, Double::parseDouble);
  }

  public double propertyAsDouble(String property, double defaultValue) {
    return propertyAs(property, defaultValue, Double::parseDouble);
  }

  public int propertyAsInt(String property) {
    return propertyAs(property, Integer::parseInt);
  }

  public int propertyAsInt(String property, int defaultValue) {
    return propertyAs(property, defaultValue, Integer::parseInt);
  }

  public long propertyAsLong(String property) {
    return propertyAs(property, Long::parseLong);
  }

  public long propertyAsLong(String property, long defaultValue) {
    return propertyAs(property, defaultValue, Long::parseLong);
  }

  public String propertyAsString(String property) {
    return propertyAs(property, Function.identity(), true);
  }

  public String propertyAsString(String property, String defaultValue) {
    return propertyAs(property, defaultValue, Function.identity(), true);
  }

  public String namespace() {
    return namespace;
  }
}
