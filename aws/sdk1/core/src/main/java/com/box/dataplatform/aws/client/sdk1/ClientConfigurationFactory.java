package com.box.dataplatform.aws.client.sdk1;

import com.amazonaws.ClientConfiguration;
import com.box.dataplatform.aws.client.ProxyConfig;

public class ClientConfigurationFactory {
  private static final int DEFAULT_SOCKET_TIMEOUT = 1000; // 1 second

  public static ClientConfiguration create(ProxyConfig proxyConfig) {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setProxyUsername(proxyConfig.username());
    clientConfiguration.setProxyPassword(proxyConfig.password());
    clientConfiguration.setProxyHost(proxyConfig.host());
    clientConfiguration.setProxyPort(proxyConfig.port());
    clientConfiguration.setSocketTimeout(DEFAULT_SOCKET_TIMEOUT);
    return clientConfiguration;
  }
}
