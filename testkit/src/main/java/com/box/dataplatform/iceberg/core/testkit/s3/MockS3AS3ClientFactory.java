package com.box.dataplatform.iceberg.core.testkit.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.box.dataplatform.iceberg.core.testkit.DPIcebergTestkit;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.S3aS3ClientFactoryShim;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provides clients and credentials for s3 clients that will interact with s3mock over http.
 */
public class MockS3AS3ClientFactory extends Configured implements S3aS3ClientFactoryShim {
  private static final Logger logger = LoggerFactory.getLogger(MockS3AS3ClientFactory.class);
  private static boolean logged = false;

  public static AWSCredentialsProvider getCredentials() {
    return new BasicAWSCredentialsProvider("foo", "bar");
  }

  @Override
  public AmazonS3 createS3Client(URI uri) throws IOException {
    String bucketName = uri.getHost();
    return getNonSecureAmazonS3Client(bucketName);
  }

  public AmazonS3 getNonSecureAmazonS3Client(String bucketName) {
    if (!logged) {
      logger.warn("test mode detected, calling getAmazonS3Client for test...");
      logged = true;
    }
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration
        .getApacheHttpClientConfig()
        .withSslSocketFactory(
            new SSLConnectionSocketFactory(
                createBlindlyTrustingSslContext(), NoopHostnameVerifier.INSTANCE));

    return AmazonS3ClientBuilder.standard()
        .withCredentials(getCredentials())
        .withClientConfiguration(clientConfiguration)
        .withEndpointConfiguration(
            getEndpointConfiguration(
                DPIcebergTestkit.REGION,
                S3MockContainer.s3mock().getHttpPort(),
                S3MockContainer.s3mock().getHttpPort(),
                false))
        .withPathStyleAccessEnabled(true)
        .build();
  }

  private AwsClientBuilder.EndpointConfiguration getEndpointConfiguration(
      String region, int port, int httpPort, boolean isSecureConnection) {
    String serviceEndpoint;
    if (isSecureConnection) {
      serviceEndpoint = "https://localhost:" + port;
    } else {
      serviceEndpoint = "http://localhost:" + port;
    }
    return new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region);
  }

  private SSLContext createBlindlyTrustingSslContext() {
    X509ExtendedTrustManager acceptAllTrustManager =
        new X509ExtendedTrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
              throws CertificateException {}

          @Override
          public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
              throws CertificateException {}

          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return null;
          }

          @Override
          public void checkClientTrusted(
              X509Certificate[] x509Certificates, String s, Socket socket)
              throws CertificateException {}

          @Override
          public void checkServerTrusted(
              X509Certificate[] x509Certificates, String s, Socket socket)
              throws CertificateException {}

          @Override
          public void checkClientTrusted(
              X509Certificate[] x509Certificates, String s, SSLEngine sslEngine)
              throws CertificateException {}

          @Override
          public void checkServerTrusted(
              X509Certificate[] x509Certificates, String s, SSLEngine sslEngine)
              throws CertificateException {}
        };
    TrustManager[] acceptAllTrustManagers = {acceptAllTrustManager};
    try {
      SSLContext sc = SSLContext.getInstance("TLS");
      sc.init(null, acceptAllTrustManagers, new SecureRandom());
      return sc;
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }
}
