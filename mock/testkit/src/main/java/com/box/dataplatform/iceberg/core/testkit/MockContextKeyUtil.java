package com.box.dataplatform.iceberg.core.testkit;

import com.box.dataplatform.iceberg.mock.MockContextKey;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.box.dataplatform.iceberg.core.testkit.TestkitUtil.uniquify;

public class MockContextKeyUtil {
  private static final Logger log = LoggerFactory.getLogger(MockContextKeyUtil.class);
  public static final String DEFAULT_CONTEXT_PREFIX = "test";

  public static MockContextKey newContextKey(String contextPrefix) {
    return new MockContextKey(uniquify(contextPrefix));
  }

  public static MockContextKey newContextKey() {
    return new MockContextKey(uniquify(DEFAULT_CONTEXT_PREFIX));
  }

  public String getLocation(TableIdentifier tableIdentifier) {
    return String.format("/{}/{}/", tableIdentifier.namespace(), tableIdentifier.name());
  }
}
