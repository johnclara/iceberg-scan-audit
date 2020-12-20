package org.apache.iceberg.addons.testkit;

import org.apache.iceberg.addons.mock.MockContextId;

import static org.apache.iceberg.addons.testkit.Util.uniquify;

public class MockContextKeyUtil {
  public static final String DEFAULT_CONTEXT_PREFIX = "test";

  public static MockContextId newContextKey(String contextPrefix) {
    return new MockContextId(uniquify(contextPrefix));
  }

  public static MockContextId newContextKey() {
    return new MockContextId(uniquify(DEFAULT_CONTEXT_PREFIX));
  }
}
