package com.box.dataplatform.crypto;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * This file is very security and compliance sensitive, please make sure to talk to
 * security-dev@box.com before making any changes. AES/CTR/NoPadding, 32 bytes key (AES256), 16
 * bytes block, all zero IV/Nonce is used for data encryption.
 */
public abstract class Encryptor {
  /** Standard encrypt/decrypt algorithm and transformation used in Box */
  protected static final String AES_ALGORITHM = "AES";

  /** CTR CONSTANTS * */
  // Size of the cipher block
  public static final int AES_BLOCK_SIZE = 16;

  /** Visible for testing */
  protected static final String CTR_TRANSFORMATION = "AES/CTR/NoPadding";

  protected static final String GCM_TRANSFORMATION = "AES/GCM/NoPadding";

  /** Initial value for CTR counter at start of a file. */
  private static final byte[] DEFAULT_CTR_IV = new byte[AES_BLOCK_SIZE];

  private static final IvParameterSpec DEFAULT_CTR_IV_PARAM = new IvParameterSpec(DEFAULT_CTR_IV);

  /** GCM CONSTANTS * */
  private static final long MAX_GCM_BLOCKS = (1L << 32) - 2; // 2^32 - 2

  // Since we use a unique key we can re-use the same Iv. Use 12 byte iv which is the gcm default.
  public static final int GCM_IV_LENGTH = 12;
  // Use 128 bit auth tag. This is default.
  public static final int GCM_AUTH_TAG_BYTE_LENGTH = 16;
  public static final int GCM_AUTH_TAG_BIT_LENGTH = GCM_AUTH_TAG_BYTE_LENGTH * 8;

  private static final byte[] DEFAULT_GCM_IV = new byte[GCM_IV_LENGTH];
  private static final GCMParameterSpec DEFAULT_GCM_PARAM =
      new GCMParameterSpec(GCM_AUTH_TAG_BIT_LENGTH, DEFAULT_GCM_IV);

  /** ABSTRACT METHODS * */
  protected abstract Cipher aesCipherInternal(String transform)
      throws NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException;

  /**
   * Generate numBytes of random bytes from the random source
   *
   * @param numBytes - number of bytes to generate
   * @return Array[Byte]
   */
  public abstract byte[] generateRandomBytes(int numBytes);

  /**
   * Exposes the SecureRandom source for random number generation
   *
   * @return SecureRandom
   */
  public abstract SecureRandom secureRandom();

  /** LOGIC * */
  public CipherBuilder gcmEncryptionBuilder(byte[] key) {
    return cipherBuilder().gcm().encrypt().withKey(key);
  }

  public CipherBuilder gcmDecryptionBuilder(byte[] key) {
    return cipherBuilder().gcm().decrypt().withKey(key);
  }

  public CipherBuilder ctrEncryptionBuilder(byte[] key) {
    return cipherBuilder().ctr().encrypt().withKey(key);
  }

  public CipherBuilder ctrDecryptionBuilder(byte[] key) {
    return cipherBuilder().ctr().decrypt().withKey(key);
  }

  /**
   * Visible for testing
   *
   * @return
   */
  protected CipherBuilder cipherBuilder() {
    return new CipherBuilder();
  }

  /**
   * This builder is mutable and "with" calls will return the updated object and not create a new
   * object.
   */
  public class CipherBuilder {
    private Integer mode = null;
    private String transform = null;
    private byte[] key = null;
    private byte[] iv = null;
    private Long offset = null;

    private CipherBuilder() {}

    private CipherBuilder withMode(int mode) {
      if (this.mode != null) {
        throw new IllegalArgumentException("Encryption mode already set");
      }
      this.mode = mode;
      return this;
    }

    /**
     * Visible for testing
     *
     * @return
     */
    protected CipherBuilder encrypt() {
      return withMode(Cipher.ENCRYPT_MODE);
    }

    /**
     * Visible for testing
     *
     * @return
     */
    protected CipherBuilder decrypt() {
      return withMode(Cipher.DECRYPT_MODE);
    }

    /**
     * Visible for testing
     *
     * @return
     */
    protected CipherBuilder withTransform(String transform) {
      if (this.transform != null) {
        throw new IllegalArgumentException("Format already set");
      }
      this.transform = transform;
      return this;
    }

    /**
     * Visible for testing
     *
     * @return
     */
    protected CipherBuilder ctr() {
      return withTransform(CTR_TRANSFORMATION);
    }

    /**
     * Visible for testing
     *
     * @return
     */
    protected CipherBuilder gcm() {
      return withTransform(GCM_TRANSFORMATION);
    }

    /**
     * Visible for testing
     *
     * @return
     */
    protected CipherBuilder withKey(byte[] key) {
      if (this.key != null) {
        throw new IllegalArgumentException("Key already set");
      }
      this.key = key;
      return this;
    }

    public CipherBuilder withIv(byte[] iv) {
      if (this.iv != null) {
        throw new IllegalArgumentException("Iv already set");
      }
      this.iv = iv;
      return this;
    }

    public CipherBuilder withOffset(long offset) {
      if (this.offset != null) {
        throw new IllegalArgumentException("Offset already set");
      }
      this.offset = offset;
      return this;
    }

    public Cipher build() {
      if (mode == null) {
        throw new IllegalArgumentException("Mode must be set");
      }
      if (transform == null) {
        throw new IllegalArgumentException("Transform must be set");
      }
      if (key == null) {
        throw new IllegalArgumentException("Key must be set");
      }
      AlgorithmParameterSpec algorithmSpec;
      if (transform.equals(CTR_TRANSFORMATION)) {
        if (iv == null) {
          iv = DEFAULT_CTR_IV;
        } else if (iv.length != AES_BLOCK_SIZE) {
          throw new IllegalArgumentException(
              String.format("CTR IV length must be equal to %d", AES_BLOCK_SIZE));
        }
        if (offset == null) {
          algorithmSpec = new IvParameterSpec(iv);
        } else {
          try {
            return aesCtrCipherWithOffset(key, iv, offset, mode);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      } else if (transform.equals(GCM_TRANSFORMATION)) {
        if (iv == null) {
          iv = DEFAULT_GCM_IV;
        } else if (iv.length != GCM_IV_LENGTH) {
          throw new IllegalArgumentException(
              String.format("GCM IV length must be equal to %d", GCM_IV_LENGTH));
        }

        if (offset == null) {
          algorithmSpec = new GCMParameterSpec(GCM_AUTH_TAG_BIT_LENGTH, iv);
        } else {
          if (mode != Cipher.DECRYPT_MODE) {
            throw new IllegalArgumentException(
                "GCM Cipher can only have an offset during decryption");
          }
          return aesGcmAsCtrCipherWithOffset(key, iv, offset);
        }
      } else {
        throw new IllegalArgumentException(String.format("Unknown transform %s", transform));
      }
      SecretKeySpec keySpec = new SecretKeySpec(key, AES_ALGORITHM);
      Cipher cipher;
      try {
        cipher = aesCipherInternal(transform);
        cipher.init(mode, keySpec, algorithmSpec);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return cipher;
    }
  }

  private Cipher aesGcmAsCtrCipherWithOffset(byte[] key, byte[] iv, long offset) {
    long blockOffset = offset / AES_BLOCK_SIZE;
    byte[] J0 = computeJ0(iv);
    byte[] newIv = incrementBlocks(J0, blockOffset);
    Cipher cipher = new CipherBuilder().decrypt().ctr().withKey(key).withIv(newIv).build();

    // Skip to the offset
    int skip = (int) (offset % AES_BLOCK_SIZE);
    byte[] skipBuffer = new byte[skip];
    try {
      cipher.update(skipBuffer, 0, skip, skipBuffer);
    } catch (ShortBufferException shortBufferException) {
      shortBufferException.printStackTrace();
    }
    Arrays.fill(skipBuffer, (byte) 0);
    return cipher;
  }

  /*
  Adapted from the AWS SDK com.amazonaws.services.s3.internal.crypto.AesCtr

  From http://csrc.nist.gov/publications/nistpubs/800-38D/SP-800-38D.pdf
  NIST Special Publication 800-38D.</a> for the definition of J0, the "pre-counter block":

  In Step 1, the hash subkey for the GHASH function is generated by applying the block cipher to
  the “zero” block. In Step 2, the pre-counter block (J0) is generated from the IV. In particular,
  when the length of the IV is 96 bits, then the padding string 031||1 is appended to the IV to form
  the pre-counter block. Otherwise, the IV is padded with the minimum number of ‘0’ bits,
  possibly none, so that the length of the resulting string is a multiple of 128 bits (the block size);
  this string in turn is appended with 64 additional ‘0’ bits, followed by the 64-bit representation of
  the length of the IV, and the GHASH function is applied to the resulting string to form the precounter block.
  In Step 3, the 32-bit incrementing function is applied to the pre-counter block to
  produce the initial counter block for an invocation of the GCTR function on the plaintext. The
  output of this invocation of the GCTR function is the ciphertext.
   */
  private byte[] computeJ0(byte[] nonce) {
    byte[] J0 = new byte[AES_BLOCK_SIZE];
    System.arraycopy(nonce, 0, J0, 0, nonce.length);
    J0[AES_BLOCK_SIZE - 1] = 0x01;
    return incrementBlocks(J0, 1);
  }

  /**
   * Increment the rightmost 32 bits of a 16-byte counter by the specified delta. Both the specified
   * delta and the resultant value must stay within the capacity of 32 bits. (Package private for
   * testing purposes.)
   *
   * @param counter a 16-byte counter used in AES/CTR
   * @param blockDelta the number of blocks (16-byte) to increment
   */
  private byte[] incrementBlocks(byte[] counter, long blockDelta) {
    if (blockDelta != 0L) {
      if (counter == null || counter.length != 16) {
        throw new IllegalArgumentException();
      }
      // Can optimize this later.  KISS for now.
      if (blockDelta > MAX_GCM_BLOCKS) {
        throw new IllegalStateException();
      }
      // Allocate 8 bytes for a long
      ByteBuffer bb = ByteBuffer.allocate(8);
      // Copy the right-most 32 bits from the counter
      for (int i = 12; i < 16; i++) {
        bb.put(i - 8, counter[i]);
      }
      long val = bb.getLong() + blockDelta; // increment by delta
      if (val > MAX_GCM_BLOCKS) {
        throw new IllegalStateException(); // overflow 2^32-2
      }
      bb.rewind();
      // Get the incremented value (result) as an 8-byte array
      byte[] result = bb.putLong(val).array();
      // Copy the rightmost 32 bits from the resultant array to the input counter;
      for (int i = 12; i < 16; i++) {
        counter[i] = result[i - 8];
      }
    }
    return counter;
  }

  /**
   * WARNING: This method is not used.
   *
   * @param key
   * @param iv
   * @param offset
   * @param cipherMode
   * @return
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws NoSuchPaddingException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchProviderException
   */
  private Cipher aesCtrCipherWithOffset(byte[] key, byte[] iv, long offset, int cipherMode)
      throws InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException,
          NoSuchAlgorithmException, NoSuchProviderException {
    if (offset % AES_BLOCK_SIZE != 0) {
      throw new IllegalArgumentException(
          String.format("The offset must be divisible by the AES block size (%d)", AES_BLOCK_SIZE));
    }
    Cipher cipher = aesCipherInternal(CTR_TRANSFORMATION);
    // The CTR mode puts the counter in the lower bits of the IV which can overflow into the nonce
    // http://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_.28CTR.29

    BigInteger ivAsBigInt = new BigInteger(1, iv);
    BigInteger newIvAsBigInt = ivAsBigInt.add(BigInteger.valueOf(offset / AES_BLOCK_SIZE));
    byte[] newIvAsCompactByteArray = newIvAsBigInt.toByteArray();
    // This handles overflow. To see why, please see com/sun/crypto/provider/CounterMode.java
    int diff = newIvAsCompactByteArray.length - AES_BLOCK_SIZE;
    byte[] newIv;
    if (diff > 0) {
      newIv = ArrayUtil.drop(diff, newIvAsCompactByteArray);
    } else if (diff < 0) {
      int padLength = 0 - diff;
      newIv = ArrayUtil.padLeft(padLength, newIvAsCompactByteArray);
    } else {
      newIv = newIvAsCompactByteArray;
    }
    cipher.init(cipherMode, new SecretKeySpec(key, AES_ALGORITHM), new IvParameterSpec(newIv));
    return cipher;
  }
}
