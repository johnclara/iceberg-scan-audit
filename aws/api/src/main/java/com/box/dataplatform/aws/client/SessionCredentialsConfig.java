package com.box.dataplatform.aws.client;

import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.Dumpable;
import com.box.dataplatform.util.LoadableBuilder;
import java.io.Serializable;
import java.util.Objects;

public class SessionCredentialsConfig implements Serializable, Dumpable {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String STS_SESSION_NAME = "sts.session.name";

  public static final String IAM_ROLE_ARN = "iam.role.arn";

  /** * PRIVATE VARIABLES ** */
  private final String sessionName;

  private final String iamRoleArn;

  /** * CONSTRUCTOR ** */
  public SessionCredentialsConfig(String sessionName, String iamRoleArn) {
    if (sessionName == null) {
      throw new IllegalArgumentException("Session name is null");
    }
    if (iamRoleArn == null) {
      throw new IllegalArgumentException("Iam role arn is null");
    }
    this.sessionName = sessionName;
    this.iamRoleArn = iamRoleArn;
  }

  /** * BUILDER ** */
  public static SessionCredentialsConfig load(Conf conf) {
    return builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, SessionCredentialsConfig> {
    public Builder() {}

    private boolean enabled = false;
    private String sessionName;
    private String iamRoleArn;

    public Builder load(Conf conf) {
      enabled = conf.containsKey(STS_SESSION_NAME);
      if (enabled) {
        sessionName = conf.propertyAsString(STS_SESSION_NAME);
        iamRoleArn = conf.propertyAsString(IAM_ROLE_ARN);
      }
      return this;
    }

    public SessionCredentialsConfig build() {
      if (!enabled) {
        return null;
      } else {
        return new SessionCredentialsConfig(sessionName, iamRoleArn);
      }
    }
  }

  /** * DUMPER ** */
  public void dump(Conf conf) {
    conf.setString(STS_SESSION_NAME, sessionName);
    conf.setString(IAM_ROLE_ARN, iamRoleArn);
  }

  /** * GETTERS ** */
  public String sessionName() {
    return sessionName;
  }

  public String iamRoleArn() {
    return iamRoleArn;
  }

  /** * EQUALS HASH CODE ** */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SessionCredentialsConfig)) return false;
    SessionCredentialsConfig that = (SessionCredentialsConfig) o;
    return sessionName.equals(that.sessionName) && iamRoleArn.equals(that.iamRoleArn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sessionName, iamRoleArn);
  }

  /** * TO STRING ** */
  @Override
  public String toString() {
    return "SessionCredentialsConfig{"
        + "sessionName='"
        + sessionName
        + '\''
        + ", iamRoleArn='"
        + iamRoleArn
        + '\''
        + '}';
  }
}
