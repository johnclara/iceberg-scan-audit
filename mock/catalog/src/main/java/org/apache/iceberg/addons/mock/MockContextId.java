package org.apache.iceberg.addons.mock;

import java.io.Serializable;
import java.util.Objects;

public class MockContextId implements Serializable {
  private final String mockContext;

  public MockContextId(String mockContext) {
    this.mockContext = mockContext;
  }

  public String mockContext() {
    return mockContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MockContextId)) return false;
    MockContextId that = (MockContextId) o;
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
