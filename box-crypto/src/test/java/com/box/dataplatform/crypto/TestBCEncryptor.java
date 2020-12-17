package com.box.dataplatform.crypto;

public class TestBCEncryptor extends TestEncryptor {
  @Override
  protected Encryptor getEncryptor() {
    return BCEncryptor.INSTANCE;
  }
}
