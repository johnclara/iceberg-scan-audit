package com.box.dataplatform.crypto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Arrays;
import javax.crypto.*;
import org.junit.Assert;
import org.junit.Test;

public class TestBCEncryptorAdditional {
  // generate a random dek
  BCEncryptor bcEncryptor = BCEncryptor.INSTANCE;
  int dekLength = 32;
  int authTagLength = 16;

  @Test
  public void testGenerateRandomByteArray() {
    int numBytes = 16;
    byte[] output = bcEncryptor.generateRandomBytes(16);
    Assert.assertEquals(numBytes, output.length);
  }

  @Test
  public void testAesEncryptDecrypt()
      throws NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException,
          InvalidAlgorithmParameterException, InvalidKeyException, IOException {
    int N = 2000;
    byte[] key = bcEncryptor.generateRandomBytes(dekLength);
    byte[] clear = bcEncryptor.generateRandomBytes(N);

    Cipher encryptor = bcEncryptor.ctrEncryptionBuilder(key).build();

    ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream(N + authTagLength);
    CipherOutputStream encryptedCipherStream =
        new CipherOutputStream(encryptedOutputStream, encryptor);
    encryptedCipherStream.write(clear);
    encryptedCipherStream.flush();
    encryptedCipherStream.close();
    byte[] encrypted = encryptedOutputStream.toByteArray();
    Assert.assertFalse(Arrays.equals(clear, encrypted));

    byte[] unencrypted = new byte[N];

    Cipher decryptor = bcEncryptor.ctrDecryptionBuilder(key).build();
    CipherInputStream decryptedCipherStream =
        new CipherInputStream(new ByteArrayInputStream(encrypted), decryptor);

    int start = 0;
    while (start < N) {
      int amountRead = decryptedCipherStream.read(unencrypted, start, N - start);
      if (amountRead <= -1) {
        throw new IllegalStateException("Didn't read the full stream");
      }
      start += amountRead;
    }

    Assert.assertArrayEquals(clear, unencrypted);
  }

  @Test
  public void testAesGcmEncryptDecrypt()
      throws NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException,
          InvalidAlgorithmParameterException, InvalidKeyException, IOException {
    byte[] key = bcEncryptor.generateRandomBytes(dekLength);
    int N = 2000;
    byte[] clear = bcEncryptor.generateRandomBytes(N);

    Cipher encryptor = bcEncryptor.gcmEncryptionBuilder(key).build();
    ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream(N + authTagLength);
    CipherOutputStream encryptedCipherStream =
        new CipherOutputStream(encryptedOutputStream, encryptor);
    encryptedCipherStream.write(clear);
    encryptedCipherStream.flush();
    encryptedCipherStream.close();
    byte[] encrypted = encryptedOutputStream.toByteArray();

    Assert.assertFalse(Arrays.equals(clear, encrypted));
    Assert.assertEquals(N + authTagLength, encrypted.length);

    byte[] unencrypted = new byte[N];

    Cipher decryptor = bcEncryptor.gcmDecryptionBuilder(key).build();
    CipherInputStream decryptedCipherStream =
        new CipherInputStream(new ByteArrayInputStream(encrypted), decryptor);

    int start = 0;
    while (start < N) {
      int amountRead = decryptedCipherStream.read(unencrypted, start, N - start);
      if (amountRead <= -1) {
        throw new IllegalStateException("Didn't read the full stream");
      }
      start += amountRead;
    }
    Assert.assertArrayEquals(clear, unencrypted);
  }

  @Test
  public void testAesGcmAsCtrEncryptDecrypt()
      throws NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException,
          InvalidAlgorithmParameterException, InvalidKeyException, IOException,
          ShortBufferException {
    byte[] key = bcEncryptor.generateRandomBytes(dekLength);
    int N = 2000;
    byte[] clear = bcEncryptor.generateRandomBytes(N);

    Cipher encryptor = bcEncryptor.gcmEncryptionBuilder(key).build();
    ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream(N + authTagLength);
    CipherOutputStream encryptedCipherStream =
        new CipherOutputStream(encryptedOutputStream, encryptor);
    encryptedCipherStream.write(clear);
    encryptedCipherStream.flush();
    encryptedCipherStream.close();
    byte[] encrypted = encryptedOutputStream.toByteArray();

    byte[] truncated = new byte[N];
    System.arraycopy(encrypted, 0, truncated, 0, N);

    Assert.assertFalse(Arrays.equals(clear, encrypted));

    byte[] unencrypted = new byte[N];

    Cipher decryptor = bcEncryptor.gcmDecryptionBuilder(key).withOffset(0).build();
    CipherInputStream decryptedCipherStream =
        new CipherInputStream(new ByteArrayInputStream(encrypted), decryptor);

    int start = 0;
    while (start < N) {
      int amountRead = decryptedCipherStream.read(unencrypted, start, N - start);
      if (amountRead <= -1) {
        throw new IllegalStateException("Didn't read the full stream");
      }
      start += amountRead;
    }

    Assert.assertArrayEquals(clear, unencrypted);
  }

  @Test
  public void testAesGcmAsCtrEncryptDecryptWithOffset()
      throws NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException,
          InvalidAlgorithmParameterException, InvalidKeyException, IOException,
          ShortBufferException {
    byte[] key = bcEncryptor.generateRandomBytes(dekLength);
    int N = 2000;
    byte[] clear = bcEncryptor.generateRandomBytes(N);

    Cipher encryptor = bcEncryptor.gcmEncryptionBuilder(key).build();
    ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream(N + authTagLength);
    CipherOutputStream encryptedCipherStream =
        new CipherOutputStream(encryptedOutputStream, encryptor);
    encryptedCipherStream.write(clear);
    encryptedCipherStream.flush();
    encryptedCipherStream.close();
    byte[] encrypted = encryptedOutputStream.toByteArray();

    Assert.assertFalse(Arrays.equals(clear, encrypted));

    byte[] unencrypted = new byte[N];

    int start = 600;
    Arrays.fill(clear, 0, start, (byte) 0);

    Cipher decryptor = bcEncryptor.gcmDecryptionBuilder(key).withOffset(start).build();
    CipherInputStream decryptedCipherStream =
        new CipherInputStream(
            new ByteArrayInputStream(encrypted, start, encrypted.length), decryptor);

    while (start < N) {
      int amountRead = decryptedCipherStream.read(unencrypted, start, N - start);
      if (amountRead <= -1) {
        throw new IllegalStateException("Didn't read the full stream");
      }
      start += amountRead;
    }

    Assert.assertArrayEquals(clear, unencrypted);
  }
}
