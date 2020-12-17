package com.box.dataplatform.util;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestProperties {
  private final String prop = "testProp";

  private Map<String, String> map;
  private Properties props;
  private Properties namespacedProps;

  @Before
  public void setUp() {
    map = new HashMap<>();
    props = Properties.of("", map);
    namespacedProps = Properties.of(map);
  }

  @Test
  public void testSetBoolean() {
    props.setBoolean(prop, true);
    Assert.assertEquals("true", map.get(prop));
  }

  @Test
  public void testSetDouble() {
    props.setDouble(prop, 1);
    Assert.assertEquals("1.0", map.get(prop));
  }

  @Test
  public void testSetInt() {
    props.setInt(prop, 1);
    Assert.assertEquals("1", map.get(prop));
  }

  @Test
  public void testSetLong() {
    props.setLong(prop, 1);
    Assert.assertEquals("1", map.get(prop));
  }

  @Test
  public void testSetString() {
    props.setString(prop, "hello");
    Assert.assertEquals("hello", map.get(prop));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNullString() {
    props.setString(prop, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void propertyAsBooleanMissing() {
    props.propertyAsBoolean(prop);
  }

  @Test(expected = IllegalArgumentException.class)
  public void propertyAsDoubleMissing() {
    props.propertyAsDouble(prop);
  }

  @Test(expected = IllegalArgumentException.class)
  public void propertyAsIntMissing() {
    props.propertyAsInt(prop);
  }

  @Test(expected = IllegalArgumentException.class)
  public void propertyAsLongMissing() {
    props.propertyAsLong(prop);
  }

  @Test(expected = IllegalArgumentException.class)
  public void propertyAsStringMissing() {
    props.propertyAsString(prop);
  }

  @Test
  public void propertyAsBooleanDefault() {
    Assert.assertTrue(props.propertyAsBoolean(prop, true));
  }

  @Test
  public void propertyAsDoubleDefault() {
    Assert.assertEquals(1, props.propertyAsDouble(prop, 1), 0);
  }

  @Test
  public void propertyAsIntDefault() {
    Assert.assertEquals(1, props.propertyAsInt(prop, 1));
  }

  @Test
  public void propertyAsLongDefault() {
    Assert.assertEquals(1, props.propertyAsLong(prop, 1));
  }

  @Test
  public void propertyAsStringDefault() {
    Assert.assertNull(props.propertyAsString(prop, null));
    Assert.assertEquals("hello", props.propertyAsString(prop, "hello"));
  }

  @Test
  public void propertyAsBoolean() {
    map.put(prop, "true");
    Assert.assertTrue(props.propertyAsBoolean(prop));
  }

  @Test
  public void propertyAsDouble() {
    map.put(prop, "1.0");
    Assert.assertEquals(1, props.propertyAsDouble(prop), 0);
  }

  @Test
  public void propertyAsInt() {
    map.put(prop, "1");
    Assert.assertEquals(1, props.propertyAsInt(prop));
  }

  @Test
  public void propertyAsLong() {
    map.put(prop, "1");
    Assert.assertEquals(1, props.propertyAsLong(prop));
  }

  @Test
  public void propertyAsString() {
    map.put(prop, "hello");
    Assert.assertEquals("hello", props.propertyAsString(prop));
  }

  @Test
  public void propertyAsStringCaseInsensitive() {
    map.put("testprop", "hello");
    Assert.assertEquals("hello", props.propertyAsString(prop));
  }

  @Test
  public void namespacedProps() {
    map.put(Conf.DEFAULT_NAMESPACE + prop, "hello");
    Assert.assertEquals("hello", namespacedProps.propertyAsString(prop));
    namespacedProps.setInt("otherProp", 1);
    Assert.assertEquals("1", map.get(Conf.DEFAULT_NAMESPACE + "otherProp"));
  }
}
