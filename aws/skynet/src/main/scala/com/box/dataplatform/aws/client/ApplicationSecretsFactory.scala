package com.box.dataplatform.aws.client

import java.io.File

import com.box.secrets.ApplicationSecrets
import com.box.secrets.vault.{VaultSecrets, VaultTokenLoader}
import com.box.secrets.vault.conf.VaultClientConfig

object ApplicationSecretsFactory {
  def create(vaultConfig: VaultConfig): ApplicationSecrets = {
    val vaultTokenLoader = new VaultTokenLoader(vaultConfig.serviceName, vaultConfig.tokenPath)
    val vaultClientConfig = VaultClientConfig(vaultConfig.host, "", new File(vaultConfig.pemPath), "")
    val applicationSecrets = VaultSecrets(vaultClientConfig, vaultTokenLoader).get
    applicationSecrets
  }
}
