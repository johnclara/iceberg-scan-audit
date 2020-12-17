package com.box.dataplatform.crypto;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

public class DefaultEncryptor extends Encryptor {
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @Override
  protected Cipher aesCipherInternal(String transform)
      throws NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException {
    return Cipher.getInstance(transform);
  }

  @Override
  public byte[] generateRandomBytes(int numBytes) {
    byte[] bytes = new byte[numBytes];
    SECURE_RANDOM.nextBytes(bytes);
    return bytes;
  }

  @Override
  public SecureRandom secureRandom() {
    return SECURE_RANDOM;
  }
}
