package org.apache.iceberg.addons.testkit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Util {
  public static String uniquify(String prefix) {
    String uuid = UUID.randomUUID().toString().replaceAll("-", "_");
    return String.format("%s_%s", prefix, uuid);
  }
}
