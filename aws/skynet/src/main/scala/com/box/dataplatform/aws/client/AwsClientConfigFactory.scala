package com.box.dataplatform.aws.client

import com.box.data.platform.model.Env
import com.box.data.platform.model.aws.Region
import com.box.data.platform.model.tenant.{RegionEnv, Tenant}
import com.box.secrets.ApplicationSecrets

object AwsClientConfigFactory {
  def load(): AwsClientConfigFactory = {
    val vaultConfig = VaultConfig.load()
    val applicationSecrets = ApplicationSecretsFactory.create(vaultConfig)
    load(applicationSecrets)
  }

  def load(applicationSecrets: ApplicationSecrets): AwsClientConfigFactory = {
    val region = sys.env("AWS_REGION")
    val env = sys.env("ENV")
    AwsClientConfigFactory(applicationSecrets, Env.fromValue(env), Region.fromValue(region))
  }
}

case class AwsClientConfigFactory(applicationSecrets: ApplicationSecrets, env: Env, region: Region) {
  def create(tenant: Tenant, role: TenantRole): AwsClientConfig = {
    val awsCredentialsFactory = AwsCredentialsFactory.load(applicationSecrets)
    val proxyConfigFactory = ProxyConfigFactory.create(applicationSecrets)
    AwsClientConfig
      .builder()
      .setRegion(region.value)
      .setStaticCredentials(awsCredentialsFactory.createStatic(tenant, role))
      .setSessionCredentialsConfig(awsCredentialsFactory.create(tenant, role))
      .setProxyConfig(ProxyConfigFactory.create(applicationSecrets).orNull)
      .build()
  }
}
