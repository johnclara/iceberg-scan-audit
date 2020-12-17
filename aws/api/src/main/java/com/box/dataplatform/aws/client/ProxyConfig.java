package com.box.dataplatform.aws.client;

import com.box.dataplatform.util.Conf;
import com.box.dataplatform.util.Dumpable;
import com.box.dataplatform.util.LoadableBuilder;
import java.io.Serializable;
import java.util.Objects;

public class ProxyConfig implements Serializable, Dumpable {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String PROXY_USERNAME = "proxy.username";

  public static final String PROXY_PASSWORD = "proxy.password";
  public static final String PROXY_HOST = "proxy.host";
  public static final String PROXY_PORT = "proxy.port";

  /** * PRIVATE VARIABLES ** */
  private final String username;

  private final String password;
  private final String host;
  private final int port;

  /** * CONSTRUCTOR ** */
  public ProxyConfig(String username, String password, String host, int port) {
    if (username == null) {
      throw new IllegalArgumentException("Username is null");
    }
    if (password == null) {
      throw new IllegalArgumentException("Password is null");
    }
    if (host == null) {
      throw new IllegalArgumentException("Host is null");
    }
    this.username = username;
    this.password = password;
    this.host = host;
    this.port = port;
  }

  /** * BUILDER ** */
  public static ProxyConfig load(Conf conf) {
    return new ProxyConfig.Builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<ProxyConfig.Builder, ProxyConfig> {
    public Builder() {}

    private boolean enabled = false;
    private String username;
    private String password;
    private String host;
    private int port;

    public Builder load(Conf conf) {
      enabled = conf.containsKey(PROXY_USERNAME);
      if (enabled) {
        username = conf.propertyAsString(PROXY_USERNAME);
        password = conf.propertyAsString(PROXY_PASSWORD);
        host = conf.propertyAsString(PROXY_HOST);
        port = conf.propertyAsInt(PROXY_PORT);
      }
      return this;
    }

    public ProxyConfig build() {
      if (!enabled) {
        return null;
      } else {
        return new ProxyConfig(username, password, host, port);
      }
    }
  }

  /** * DUMPER ** */
  public void dump(Conf conf) {
    conf.setString(PROXY_USERNAME, username);
    conf.setString(PROXY_PASSWORD, password);
    conf.setString(PROXY_HOST, host);
    conf.setInt(PROXY_PORT, port);
  }

  /** * GETTERS ** */
  public String username() {
    return username;
  }

  public String password() {
    return password;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  /** * EQUALS HASH CODE ** */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ProxyConfig)) return false;
    ProxyConfig that = (ProxyConfig) o;
    return port == that.port
        && username.equals(that.username)
        && password.equals(that.password)
        && host.equals(that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password, host, port);
  }

  /** TO STRING WARNING: password should be redacted */
  @Override
  public String toString() {
    return "ProxyConfig{"
        + "username='"
        + username
        + '\''
        + ", password='"
        + "REDACTED"
        + '\''
        + ", host='"
        + host
        + '\''
        + ", port="
        + port
        + '}';
  }
}
