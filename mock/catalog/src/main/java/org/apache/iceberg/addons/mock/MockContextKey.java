package org.apache.iceberg.addons.mock;

import java.io.Serializable;
import java.util.Objects;

public class MockContextKey implements Serializable {
  private final String mockContext;

  public MockContextKey(String mockContext) {
    this.mockContext = mockContext;
  }

  public String mockContext() {
    return mockContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MockContextKey)) return false;
    MockContextKey that = (MockContextKey) o;
    return Objects.equals(mockContext, that.mockContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mockContext);
  }

  @Override
  public String toString() {
    return "MockContext{" +
        "mockContext='" + mockContext + '\'' +
        '}';
  }
}
