package com.box.dataplatform.aws.client;

import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.Dumpable;
import com.box.dataplatform.util.LoadableBuilder;
import java.io.Serializable;
import java.util.Objects;

public class AwsClientConfig implements Serializable, Dumpable {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String AWS_REGION = "aws.region";

  public static final String DEFAULT_REGION = "us-west-2";

  /** * PRIVATE VARIABLES ** */
  private final String region;

  private final StaticCredentials staticCredentials;
  private final SessionCredentialsConfig sessionCredentialsConfig;
  private final ProxyConfig proxyConfig;

  /** * CONSTRUCTOR ** */
  public AwsClientConfig(
      String region,
      StaticCredentials staticCredentials,
      SessionCredentialsConfig sessionCredentialsConfig,
      ProxyConfig proxyConfig) {
    if (region == null) {
      throw new IllegalArgumentException("Region is null");
    }
    this.region = region;
    this.staticCredentials = staticCredentials;
    this.sessionCredentialsConfig = sessionCredentialsConfig;
    this.proxyConfig = proxyConfig;
  }

  /** * BUILDER ** */
  public static AwsClientConfig load(Conf conf) {
    return builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, AwsClientConfig> {
    public Builder() {}

    String region = DEFAULT_REGION;
    StaticCredentials staticCredentials;
    SessionCredentialsConfig sessionCredentialsConfig;
    ProxyConfig proxyConfig;

    public Builder setRegion(String region) {
      this.region = region;
      return this;
    }

    public Builder setStaticCredentials(StaticCredentials staticCredentials) {
      this.staticCredentials = staticCredentials;
      return this;
    }

    public Builder setSessionCredentialsConfig(SessionCredentialsConfig sessionCredentialsConfig) {
      this.sessionCredentialsConfig = sessionCredentialsConfig;
      return this;
    }

    public Builder setProxyConfig(ProxyConfig proxyConfig) {
      this.proxyConfig = proxyConfig;
      return this;
    }

    public Builder load(Conf conf) {
      region = conf.propertyAsString(AWS_REGION, DEFAULT_REGION);
      staticCredentials = StaticCredentials.load(conf);
      sessionCredentialsConfig = SessionCredentialsConfig.load(conf);
      proxyConfig = ProxyConfig.load(conf);
      return this;
    }

    public AwsClientConfig build() {
      return new AwsClientConfig(region, staticCredentials, sessionCredentialsConfig, proxyConfig);
    }
  }

  /** * DUMPER ** */
  public void dump(Conf conf) {
    conf.setString(AWS_REGION, region);
    if (staticCredentials != null) {
      staticCredentials.dump(conf);
    }
    if (sessionCredentialsConfig != null) {
      sessionCredentialsConfig.dump(conf);
    }
    if (proxyConfig != null) {
      proxyConfig.dump(conf);
    }
  }

  /** * GETTERS ** */
  public String region() {
    return region;
  }

  public StaticCredentials staticAwsCredentials() {
    return staticCredentials;
  }

  public SessionCredentialsConfig sessionCredentialsConfig() {
    return sessionCredentialsConfig;
  }

  public ProxyConfig proxyConfig() {
    return proxyConfig;
  }

  /** * EQUALS HASH CODE ** */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AwsClientConfig)) return false;
    AwsClientConfig that = (AwsClientConfig) o;
    return region.equals(that.region)
        && Objects.equals(staticCredentials, that.staticCredentials)
        && Objects.equals(sessionCredentialsConfig, that.sessionCredentialsConfig)
        && Objects.equals(proxyConfig, that.proxyConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(region, staticCredentials, sessionCredentialsConfig, proxyConfig);
  }

  /** * TO STRING ** */
  @Override
  public String toString() {
    return "AwsClientConfig{"
        + "region='"
        + region
        + '\''
        + ", staticAwsCredentials="
        + staticCredentials
        + ", sessionCredentialsConfig="
        + sessionCredentialsConfig
        + ", proxyConfig="
        + proxyConfig
        + '}';
  }
}
