package com.box.dataplatform.util;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNestedConf {
  private final String prop = "testProp";

  private Map<String, String> map;
  private Properties props;
  private NestedConf nested;

  @Before
  public void setUp() {
    map = new HashMap<>();
    props = Properties.of("base.", map);
    nested = NestedConf.of(props, "nest");
  }

  @Test
  public void works() {
    nested.setBoolean("field", true);
    Assert.assertEquals("true", map.get("base.nest.field"));
    Assert.assertTrue(nested.propertyAsBoolean("field"));
    Assert.assertTrue(props.propertyAsBoolean("nest.field"));
  }
}
