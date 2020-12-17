package com.box.dataplatform.aws.client.sdk2;

import com.box.dataplatform.aws.client.ProxyConfig;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.ProxyConfiguration;

public class SdkAsyncHttpClientFactory {
  public static SdkAsyncHttpClient create(ProxyConfig proxyConfig) {
    ProxyConfiguration awsProxyConfig =
        ProxyConfiguration.builder()
            .username(proxyConfig.username())
            .password(proxyConfig.password())
            .host(proxyConfig.host())
            .port(proxyConfig.port())
            .build();

    return AwsCrtAsyncHttpClient.builder().proxyConfiguration(awsProxyConfig).build();
  }
}
