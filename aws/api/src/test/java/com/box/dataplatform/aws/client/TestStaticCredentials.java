package com.box.dataplatform.aws.client;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStaticCredentials {
  private StaticCredentials staticCredentials;

  @Before
  public void setUp() {
    staticCredentials = new StaticCredentials("myAccessKey", "mySecretKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullAccessKey() {
    new StaticCredentials(null, "mySecretKey");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullSecretKey() {
    new StaticCredentials("myAccessKey", null);
  }

  @Test
  public void testRedactedAccessKey() {
    Assert.assertFalse(staticCredentials.toString().contains("myAccessKey"));
  }

  @Test
  public void testRedactedSecretKey() {
    Assert.assertFalse(staticCredentials.toString().contains("mySecretKey"));
  }
}
