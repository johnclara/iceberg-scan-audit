package com.box.dataplatform.aws.client;

import org.junit.Test;

public class TestSessionCredentialsConfig {

  @Test(expected = IllegalArgumentException.class)
  public void testNullSession() {
    new SessionCredentialsConfig(null, "myArn");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullArn() {
    new SessionCredentialsConfig("mySession", null);
  }
}
