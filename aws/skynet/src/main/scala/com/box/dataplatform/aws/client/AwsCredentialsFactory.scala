package com.box.dataplatform.aws.client

import com.box.data.platform.model.tenant.Tenant
import com.box.secrets.ApplicationSecrets

object AwsCredentialsFactory {
  def load(
      applicationSecrets: ApplicationSecrets
  ): AwsCredentialsFactory = {
    val awsAccountId = sys.env("AWS_ACCOUNT_ID")
    val globalCredentialsVaultPath: String = sys.env("AWS_CREDENTIALS_VAULT_PATH")
    val tenantCredentialsBaseVaultPath: String = sys.env("TENANT_CREDENTIALS_VAULT_BASE_PATH")
    new AwsCredentialsFactory(
      applicationSecrets,
      tenantCredentialsBaseVaultPath,
      globalCredentialsVaultPath,
      awsAccountId
    )
  }
}

class AwsCredentialsFactory(
    applicationSecrets: ApplicationSecrets,
    tenantCredentialsBaseVaultPath: String,
    globalCredentialsVaultPath: String,
    awsAccountId: String
) {

  private def getRoleARN(roleName: String) = s"arn:aws:iam::$awsAccountId:role/$roleName"

  def createStatic(tenant: Tenant, role: TenantRole): StaticCredentials = {
    val tenantVaultPath = role.getVaultPath(tenant, tenantCredentialsBaseVaultPath)
    createStatic(tenantVaultPath)
  }

  def createStaticGlobal: StaticCredentials =
    createStatic(globalCredentialsVaultPath)

  def createStatic(vaultPath: String): StaticCredentials = {
    val credentials = applicationSecrets.getSecrets(vaultPath).get
    new StaticCredentials(credentials("access_key"), credentials("secret"))
  }

  def create(
      tenant: Tenant,
      role: TenantRole
  ): SessionCredentialsConfig = {
    val sessionName = role.getSessionName(tenant)
    val tenantVaultPath = role.getVaultPath(tenant, tenantCredentialsBaseVaultPath)
    val credentials = applicationSecrets.getSecrets(tenantVaultPath).get
    new SessionCredentialsConfig(
      sessionName,
      getRoleARN(role.getRoleName(tenant))
    )
  }

  def createGlobal: SessionCredentialsConfig = {
    val sessionName = "data-platform-global-emr-api"
    new SessionCredentialsConfig(
      sessionName,
      getRoleARN(globalCredentialsVaultPath)
    )
  }
}
