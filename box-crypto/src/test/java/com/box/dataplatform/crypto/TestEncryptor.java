package com.box.dataplatform.crypto;

import java.util.Arrays;
import java.util.Random;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** */
public abstract class TestEncryptor {
  private static final Random random = new Random();

  protected abstract Encryptor getEncryptor();

  private byte[] key;
  private byte[] ctrIv;
  private byte[] gcmIv;
  private Encryptor encryptor;
  private byte[] data;

  @Before
  public void setup() {
    encryptor = getEncryptor();
    key = randomBytes(32);
    ctrIv = randomBytes(16);
    gcmIv = randomBytes(12);
    data = randomBytes(256);
  }

  @Test
  public void generateRandomBytes() {
    byte[] random96Bytes = encryptor.generateRandomBytes(96);
    byte[] random48Bytes = encryptor.generateRandomBytes(48);
    byte[] random1Byte = encryptor.generateRandomBytes(1);

    Assert.assertEquals(96, random96Bytes.length);
    Assert.assertEquals(48, random48Bytes.length);
    Assert.assertEquals(1, random1Byte.length);
  }

  /** CTR TESTS * */
  @Test
  public void testCtrEncryptionDecryption() throws BadPaddingException, IllegalBlockSizeException {
    testEncryptionDecryption(Encryptor.CTR_TRANSFORMATION, ctrIv);
  }

  @Test
  public void testCtrEncryptionDecryptionWithOffset()
      throws BadPaddingException, IllegalBlockSizeException {
    testEncryptionDecryptionWithOffset(Encryptor.CTR_TRANSFORMATION, ctrIv);
  }

  @Test
  public void testCtrDecryptionWithOffset() throws BadPaddingException, IllegalBlockSizeException {
    testDecryptionWithOffset(Encryptor.CTR_TRANSFORMATION, ctrIv);
  }

  @Test
  public void testCtrDecryptionWithOffsetAndPadding()
      throws BadPaddingException, IllegalBlockSizeException {
    testDecryptionWithOffsetAndPadding(Encryptor.CTR_TRANSFORMATION, ctrIv);
  }

  @Test
  public void testCtrDecryptionWithDifferentIv()
      throws BadPaddingException, IllegalBlockSizeException {
    testDecryptionWithDifferentIv(Encryptor.CTR_TRANSFORMATION);
  }

  @Test(expected = Exception.class)
  public void testCtrFailDecryptionWithUnderflowingIv() {
    testFailDecryptionWithUnderflowingIv(Encryptor.CTR_TRANSFORMATION);
  }

  /** GCM TESTS * */
  @Test
  public void testGcmEncryptionDecryption() throws BadPaddingException, IllegalBlockSizeException {
    testEncryptionDecryption(Encryptor.GCM_TRANSFORMATION, gcmIv);
  }

  /**
   * Should throw because gcm encryption with offset is not supported
   *
   * @throws BadPaddingException
   * @throws IllegalBlockSizeException
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGcmEncryptionDecryptionWithOffset()
      throws BadPaddingException, IllegalBlockSizeException {
    testEncryptionDecryptionWithOffset(Encryptor.GCM_TRANSFORMATION, gcmIv);
  }

  @Test
  public void testGcmDecryptionWithOffset() throws BadPaddingException, IllegalBlockSizeException {
    testDecryptionWithOffset(Encryptor.GCM_TRANSFORMATION, gcmIv);
  }

  @Test
  public void testGcmDecryptionWithOffsetAndPadding()
      throws BadPaddingException, IllegalBlockSizeException {
    testDecryptionWithOffsetAndPadding(Encryptor.GCM_TRANSFORMATION, gcmIv);
  }

  @Test
  public void testGcmDecryptionWithDifferentIv()
      throws BadPaddingException, IllegalBlockSizeException {
    testDecryptionWithDifferentIv(Encryptor.GCM_TRANSFORMATION);
  }

  @Test(expected = Exception.class)
  public void testGcmFailDecryptionWithUnderflowingIv() {
    testFailDecryptionWithUnderflowingIv(Encryptor.GCM_TRANSFORMATION);
  }

  /** GENERIC TESTS * */
  private void testEncryptionDecryption(String transform, byte[] iv)
      throws BadPaddingException, IllegalBlockSizeException {
    Cipher dataEncryptor =
        encryptor
            .cipherBuilder()
            .withTransform(transform)
            .encrypt()
            .withKey(key)
            .withIv(iv)
            .build();
    byte[] encryptedData = dataEncryptor.doFinal(data);

    Cipher dataDecryptor =
        encryptor
            .cipherBuilder()
            .withTransform(transform)
            .decrypt()
            .withKey(key)
            .withIv(iv)
            .build();
    byte[] plaintextData = dataDecryptor.doFinal(encryptedData);
    Assert.assertArrayEquals(data, plaintextData);
  }

  private void testEncryptionDecryptionWithOffset(String transform, byte[] iv)
      throws BadPaddingException, IllegalBlockSizeException {
    int offset = 16;
    byte[] data1 = ArrayUtil.leftSplit(offset, data);
    byte[] data2 = ArrayUtil.rightSplit(offset, data);

    Cipher encryptor1 =
        encryptor
            .cipherBuilder()
            .encrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(0)
            .build();
    byte[] encryptedData1 = encryptor1.update(data1);
    byte[] iv1 = encryptor1.getIV();

    Cipher encryptor2 =
        encryptor
            .cipherBuilder()
            .encrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(offset)
            .build();

    byte[] encryptedData2 = encryptor2.update(data2);

    byte[] encryptedData = ArrayUtil.concat(encryptedData1, encryptedData2);

    byte[] encryptedDataOnce =
        encryptor
            .cipherBuilder()
            .encrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .build()
            .update(data);

    Assert.assertArrayEquals(
        "Encrypted data should be equivalent by offset and in one go",
        encryptedDataOnce,
        encryptedData);

    Cipher decryptor =
        encryptor
            .cipherBuilder()
            .decrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .build();
    byte[] decryptedData = decryptor.update(encryptedData);
    Assert.assertArrayEquals(data, decryptedData);
  }

  private void testDecryptionWithOffset(String transform, byte[] iv)
      throws BadPaddingException, IllegalBlockSizeException {
    Cipher dataEncryptor =
        encryptor
            .cipherBuilder()
            .encrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .build();
    byte[] encryptedDataWithTag = dataEncryptor.doFinal(data);
    byte[] encryptedData;
    if (transform.equals(Encryptor.GCM_TRANSFORMATION)) {
      encryptedData =
          ArrayUtil.leftSplit(
              encryptedDataWithTag.length - Encryptor.GCM_AUTH_TAG_BYTE_LENGTH,
              encryptedDataWithTag);
    } else {
      encryptedData = encryptedDataWithTag;
    }

    int offset = 128;
    byte[] data1 = ArrayUtil.leftSplit(offset, encryptedData);
    byte[] data2 = ArrayUtil.rightSplit(offset, encryptedData);

    Cipher decryptor1 =
        encryptor
            .cipherBuilder()
            .decrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(0)
            .build();
    byte[] decryptedData1 = decryptor1.doFinal(data1);

    Cipher decryptor2 =
        encryptor
            .cipherBuilder()
            .decrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(offset)
            .build();
    byte[] decryptedData2 = decryptor2.doFinal(data2);

    byte[] decryptedData = ArrayUtil.concat(decryptedData1, decryptedData2);

    Assert.assertArrayEquals(data, decryptedData);
  }

  private void testDecryptionWithOffsetAndPadding(String transform, byte[] iv)
      throws BadPaddingException, IllegalBlockSizeException {
    Cipher dataEncryptor =
        encryptor
            .cipherBuilder()
            .encrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .build();
    byte[] encryptedData = dataEncryptor.doFinal(data);
    int split = 135;
    int offset = 128;
    byte[] data1 = ArrayUtil.leftSplit(split, encryptedData);
    byte[] data2 = ArrayUtil.rightSplit(split, encryptedData);

    Cipher decryptor1 =
        encryptor
            .cipherBuilder()
            .decrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(0)
            .build();
    byte[] decryptedData1 = decryptor1.doFinal(data1);

    Cipher decryptor2 =
        encryptor
            .cipherBuilder()
            .decrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(offset)
            .build();
    byte[] paddedData2 = ArrayUtil.padLeft(7, data2);
    byte[] decryptedData2WithPad = decryptor2.doFinal(paddedData2);
    byte[] decryptedData2 =
        ArrayUtil.leftSplit(data.length - split, ArrayUtil.drop(7, decryptedData2WithPad));

    byte[] decryptedData = ArrayUtil.concat(decryptedData1, decryptedData2);
    Assert.assertArrayEquals(data, decryptedData);
  }

  private void testDecryptionWithDifferentIv(String transform)
      throws BadPaddingException, IllegalBlockSizeException {
    int n;
    if (transform.equals(Encryptor.CTR_TRANSFORMATION)) {
      n = Encryptor.AES_BLOCK_SIZE;
    } else {
      n = Encryptor.GCM_IV_LENGTH;
    }
    for (int i = 0; i < n; i++) {
      byte[] left = new byte[i];
      byte[] right = new byte[n - i];
      Arrays.fill(left, (byte) 0);
      Arrays.fill(right, (byte) 0xFF);
      testIv(transform, ArrayUtil.concat(left, right));
      Arrays.fill(left, (byte) 0xFF);
      Arrays.fill(right, (byte) 0);
      testIv(transform, ArrayUtil.concat(left, right));
    }
  }

  private void testIv(String transform, byte[] iv)
      throws BadPaddingException, IllegalBlockSizeException {
    Cipher dataEncryptor =
        encryptor
            .cipherBuilder()
            .encrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .build();
    byte[] encryptedDataWithTag = dataEncryptor.doFinal(data);
    byte[] encryptedData;
    if (transform.equals(Encryptor.GCM_TRANSFORMATION)) {
      encryptedData =
          ArrayUtil.leftSplit(
              encryptedDataWithTag.length - Encryptor.GCM_AUTH_TAG_BYTE_LENGTH,
              encryptedDataWithTag);
    } else {
      encryptedData = encryptedDataWithTag;
    }
    int split =
        ((random.nextInt(1024 * 1024) / Encryptor.AES_BLOCK_SIZE) * Encryptor.AES_BLOCK_SIZE)
            % encryptedData.length;
    byte[] data1 = ArrayUtil.leftSplit(split, encryptedData);
    byte[] data2 = ArrayUtil.rightSplit(split, encryptedData);

    Cipher decryptor1 =
        encryptor
            .cipherBuilder()
            .decrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(0)
            .build();
    byte[] decryptedData1 = decryptor1.doFinal(data1);

    Cipher decryptor2 =
        encryptor
            .cipherBuilder()
            .decrypt()
            .withTransform(transform)
            .withKey(key)
            .withIv(iv)
            .withOffset(split)
            .build();
    byte[] decryptedData2 = decryptor2.doFinal(data2);

    byte[] decryptedData = ArrayUtil.concat(decryptedData1, decryptedData2);
    Assert.assertArrayEquals(data, decryptedData);
  }

  private void testFailDecryptionWithUnderflowingIv(String transform) {
    encryptor.cipherBuilder().withTransform(transform).withKey(key).withIv(new byte[8]).build();
  }

  protected byte[] randomBytes(int size) {
    byte[] arr = new byte[size];
    random.nextBytes(arr);
    return arr;
  }
}
