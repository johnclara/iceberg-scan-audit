package com.box.dataplatform.crypto;

public class TestDefaultEncryptor extends TestEncryptor {
  @Override
  protected Encryptor getEncryptor() {
    return new DefaultEncryptor();
  }
}
