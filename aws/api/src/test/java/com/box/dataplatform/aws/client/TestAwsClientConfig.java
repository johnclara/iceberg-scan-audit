package com.box.dataplatform.aws.client;

import static com.box.dataplatform.test.util.SerializableUtil.deserialize;
import static com.box.dataplatform.test.util.SerializableUtil.serialize;

import com.box.dataplatform.util.Properties;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAwsClientConfig {
  private AwsClientConfig allSet;
  private AwsClientConfig noProxy;
  private AwsClientConfig noSession;
  private List<AwsClientConfig> allConfigs;

  @Before
  public void setUp() {
    allConfigs = new ArrayList<>();
    allSet =
        AwsClientConfig.builder()
            .setRegion("myRegion")
            .setProxyConfig(new ProxyConfig("myUsername", "myPassword", "myHost", 2020))
            .setSessionCredentialsConfig(
                new SessionCredentialsConfig("mySessionName", "myIamRoleArn"))
            .setStaticCredentials(new StaticCredentials("accessKey", "secretKey"))
            .build();
    allConfigs.add(allSet);

    noProxy =
        AwsClientConfig.builder()
            .setRegion("myRegion")
            .setSessionCredentialsConfig(
                new SessionCredentialsConfig("mySessionName", "myIamRoleArn"))
            .setStaticCredentials(new StaticCredentials("accessKey", "secretKey"))
            .build();
    allConfigs.add(noProxy);

    noSession =
        AwsClientConfig.builder()
            .setRegion("myRegion")
            .setStaticCredentials(new StaticCredentials("accessKey", "secretKey"))
            .build();
    allConfigs.add(noSession);
  }

  @Test
  public void testBuilderAndEqualsWorks() {
    AwsClientConfig manualVersion =
        new AwsClientConfig(
            "myRegion",
            new StaticCredentials("accessKey", "secretKey"),
            new SessionCredentialsConfig("mySessionName", "myIamRoleArn"),
            new ProxyConfig("myUsername", "myPassword", "myHost", 2020));

    Assert.assertEquals(allSet, manualVersion);
  }

  @Test
  public void testDumpLoad() {
    for (AwsClientConfig config : allConfigs) {
      Map<String, String> props = new HashMap<>();
      config.dump(Properties.of(props));

      Map<String, String> copyProps = new HashMap<>(props);

      AwsClientConfig loaded = config.load(Properties.of(copyProps));
      Assert.assertEquals(config, loaded);
    }
  }

  @Test
  public void testSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    for (AwsClientConfig config : allConfigs) {
      byte[] serialized1 = serialize(config);
      byte[] serialized2 = serialize(config);

      Object deserialized1 = deserialize(serialized1);
      Object deserialized2 = deserialize(serialized2);
      Assert.assertEquals(deserialized1, deserialized2);
      Assert.assertEquals(config, deserialized1);
      Assert.assertEquals(config, deserialized2);
    }
  }
}
