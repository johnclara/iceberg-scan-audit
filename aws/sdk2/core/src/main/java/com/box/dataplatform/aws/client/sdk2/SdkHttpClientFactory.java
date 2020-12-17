package com.box.dataplatform.aws.client.sdk2;

import com.box.dataplatform.aws.client.AwsClientConfig;
import com.box.dataplatform.aws.client.ProxyConfig;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import org.apache.http.client.utils.URIBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

public class SdkHttpClientFactory {
  private static final LoadingCache<AwsClientConfig, SdkHttpClient> CACHE =
      Caffeine.newBuilder().softValues().build(awsClientConfig -> create(awsClientConfig));

  public static SdkHttpClient getOrCreate(AwsClientConfig awsClientConfig) {
    return CACHE.get(awsClientConfig);
  }

  private static final String DEFAULT_SCHEME = "http";
  private static final long DEFAULT_SOCKET_TIMEOUT = 1000;
  private static final boolean PREEMPTIVE_BASIC_AUTH = true;

  public static SdkHttpClient create(AwsClientConfig awsClientConfig) {
    if (awsClientConfig.proxyConfig() == null) {
      return create();
    } else {
      return create(awsClientConfig.proxyConfig());
    }
  }

  public static SdkHttpClient create(ProxyConfig proxyConfig) {
    URI proxyEndpoint;
    try {
      proxyEndpoint =
          new URIBuilder()
              .setScheme(DEFAULT_SCHEME)
              .setHost(proxyConfig.host())
              .setPort(proxyConfig.port())
              .build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid proxy config " + proxyConfig, e);
    }

    ProxyConfiguration awsProxyConfig =
        ProxyConfiguration.builder()
            .username(proxyConfig.username())
            .password(proxyConfig.password())
            .endpoint(proxyEndpoint)
            .preemptiveBasicAuthenticationEnabled(PREEMPTIVE_BASIC_AUTH)
            .build();

    return ApacheHttpClient.builder()
        .proxyConfiguration(awsProxyConfig)
        .socketTimeout(Duration.ofMillis(DEFAULT_SOCKET_TIMEOUT))
        .build();
  }

  public static SdkHttpClient create() {
    return ApacheHttpClient.builder()
        .socketTimeout(Duration.ofMillis(DEFAULT_SOCKET_TIMEOUT))
        .build();
  }
}
