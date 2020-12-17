package com.box.dataplatform.aws.client

import com.box.data.platform.model.tenant.Tenant

sealed trait Role

case object GlobalControlPlane extends Role

sealed trait TenantRole extends Role {
  def getVaultPath(tenant: Tenant, tenantCredentialsBaseVaultPath: String): String
  def getRoleName(tenant: Tenant): String
  def getSessionName(tenant: Tenant): String
}

case object TenantDataPlane extends TenantRole {
  override def getVaultPath(tenant: Tenant, tenantCredentialsBaseVaultPath: String): String =
    s"${tenantCredentialsBaseVaultPath}/shared/tenant/${tenant.id.name}/data-plane/${tenant.credentialsVersion}"

  override def getRoleName(tenant: Tenant): String = tenant.dataPlaneRole

  override def getSessionName(tenant: Tenant) = s"data-platform-data-${tenant.id.name}"
}

case object TenantControlPlane extends TenantRole {
  override def getVaultPath(tenant: Tenant, tenantCredentialsBaseVaultPath: String): String =
    s"${tenantCredentialsBaseVaultPath}/private/tenant/${tenant.id.name}/control-plane/${tenant.credentialsVersion}"

  override def getRoleName(tenant: Tenant): String = tenant.controlPlaneRole

  override def getSessionName(tenant: Tenant) = s"data-platform-control-${tenant.id.name}"
}
