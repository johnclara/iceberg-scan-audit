package com.box.dataplatform.iceberg.core.testkit.s3;

import com.adobe.testing.s3mock.S3MockApplication;
import java.util.HashMap;
import java.util.Map;

public class S3MockContainer {
  private static S3MockApplication s3mock = null;

  public static void start(String rootDir) {
    Map<String, Object> props = new HashMap<>();
    props.put("secureConnection", new Boolean(false));
    props.put("root", rootDir);
    /* TODO figure out how to do port ranges
    props.put("http.port", 0);
    props.put("server.port", 0);
     */

    // start mock s3
    s3mock = S3MockApplication.start(props);
  }

  public static void stop() {
    if (s3mock != null) {
      s3mock.stop();
    }
    {
      s3mock = null;
    }
  }

  public static S3MockApplication s3mock() {
    return s3mock;
  }
}
