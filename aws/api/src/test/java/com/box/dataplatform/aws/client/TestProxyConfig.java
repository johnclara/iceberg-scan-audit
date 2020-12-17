package com.box.dataplatform.aws.client;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestProxyConfig {
  private ProxyConfig proxyConfig;

  @Before
  public void setUp() {
    proxyConfig = new ProxyConfig("myUsername", "myPassword", "myHost", 2020);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullUsername() {
    new ProxyConfig(null, "myPassword", "myHost", 2020);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullPassword() {
    new ProxyConfig("myUsername", null, "myHost", 2020);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullHost() {
    new ProxyConfig("myUsername", "myPassword", null, 2020);
  }

  @Test
  public void testRedactedPassword() {
    Assert.assertFalse(proxyConfig.toString().contains("myPassword"));
  }
}
