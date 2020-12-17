package com.box.dataplatform.aws.client;

import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.Dumpable;
import com.box.dataplatform.util.LoadableBuilder;
import java.io.Serializable;
import java.util.Objects;

public class StaticCredentials implements Serializable, Dumpable {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String ACCESS_KEY = "creds.access.key";

  public static final String SECRET_KEY = "creds.secret.key";

  /** * PRIVATE VARIABLES ** */
  private final String accessKey;

  private final String secretKey;

  /** * CONSTRUCTOR ** */
  public StaticCredentials(String accessKey, String secretKey) {
    if (accessKey == null) {
      throw new IllegalArgumentException("Access key is null");
    }
    if (secretKey == null) {
      throw new IllegalArgumentException("Secret key is null");
    }
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  /** * BUILDER ** */
  public static StaticCredentials load(Conf conf) {
    return builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, StaticCredentials> {
    private boolean enabled = false;
    private String accessKey;
    private String secretKey;

    @Override
    public Builder load(Conf conf) {
      enabled = conf.containsKey(ACCESS_KEY);
      if (enabled) {
        accessKey = conf.propertyAsString(ACCESS_KEY);
        secretKey = conf.propertyAsString(SECRET_KEY);
      }
      return this;
    }

    @Override
    public StaticCredentials build() {
      if (!enabled) {
        return null;
      } else {
        return new StaticCredentials(accessKey, secretKey);
      }
    }
  }

  public void dump(Conf conf) {
    conf.setString(ACCESS_KEY, accessKey);
    conf.setString(SECRET_KEY, secretKey);
  }

  /** * GETTERS ** */
  public String accessKey() {
    return accessKey;
  }

  public String secretKey() {
    return secretKey;
  }

  /** * EQUALS HASH CODE ** */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StaticCredentials that = (StaticCredentials) o;
    return accessKey.equals(that.accessKey) && secretKey.equals(that.secretKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessKey, secretKey);
  }

  /** TO STRING WARNING: both accessKey and secretKey should be redacted */
  @Override
  public String toString() {
    return "StaticAwsCredentials{"
        + "accessKey='"
        + "REDACTED"
        + '\''
        + ", secretKey='"
        + "REDACTED"
        + '\''
        + '}';
  }
}
