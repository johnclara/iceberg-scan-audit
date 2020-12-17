package com.box.dataplatform.aws.client

object VaultConfig {
  def load(): VaultConfig = {
    val vaultHost = sys.env("VAULT_HOST")
    val vaultTokenPath = sys.env("VAULT_TOKEN_PATH")
    val vaultPemPath = sys.env("VAULT_PEM_PATH")
    val serviceName = sys.env("SERVICE_NAME")
    VaultConfig(vaultHost, vaultTokenPath, vaultPemPath, serviceName)
  }
}

case class VaultConfig(host: String, tokenPath: String, pemPath: String, serviceName: String)
