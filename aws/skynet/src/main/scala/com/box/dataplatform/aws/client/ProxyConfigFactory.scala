package com.box.dataplatform.aws.client

import com.box.secrets.ApplicationSecrets

object ProxyConfigFactory {
  def create(applicationSecrets: ApplicationSecrets): Option[ProxyConfig] =
    if (sys.env.getOrElse("USE_WEB_PROXY", "true").toBoolean) {
      val host = sys.env("PROXY_HOST")
      val username = sys.env("PROXY_USERNAME")
      val passwordPath = sys.env("PROXY_PASSWORD_PATH")
      val port = sys.env("PROXY_PORT").toInt
      val password = applicationSecrets.getSecret(passwordPath).get
      Some(
        new ProxyConfig(
          username,
          password,
          host,
          port
        ))
    } else {
      None
    }
}
