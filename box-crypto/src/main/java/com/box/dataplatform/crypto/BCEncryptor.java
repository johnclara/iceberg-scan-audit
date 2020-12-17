package com.box.dataplatform.crypto;

import java.rmi.dgc.VMID;
import java.security.*;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import org.bouncycastle.crypto.fips.FipsDRBG;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

/**
 * This file is very security and compliance sensitive, please make sure to talk to
 * security-dev@box.com before making any changes.
 */
public class BCEncryptor extends com.box.dataplatform.crypto.Encryptor {
  private static final String BC_FIPS_JCE_PROVIDER_NAME = "BCFIPS";
  private static final int DEFAULT_SECURITY_STRENGTH_BYTES = 256 / 8;
  private static final String DEFAULT_ADDITIONAL_INPUT_STRING = "Box FIPS DRBG";
  private static final String DEFAULT_ALGORITHM = "DEFAULT";

  public static final BCEncryptor INSTANCE = new BCEncryptor();

  private final Provider provider;
  private final SecureRandom secureRandom;

  private BCEncryptor() {
    provider = new BouncyCastleFipsProvider();
    Security.addProvider(provider);
    this.secureRandom = getSecureRandom();
  }

  // Setup the random number generator
  // Page 43 of https://cloud.box.com/s/2lyh0to9uf8u8nrj1qimakyt57g5ntk3
  private static SecureRandom getSecureRandom() {
    boolean predictionResistant = false;
    byte[] additionalInput = DEFAULT_ADDITIONAL_INPUT_STRING.getBytes();
    byte[] personalizationString = new VMID().toString().getBytes();
    SecureRandom entropySource; // blocking due to prediction resistence
    try {
      entropySource = SecureRandom.getInstance(DEFAULT_ALGORITHM, BC_FIPS_JCE_PROVIDER_NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // predictionResistant specify whether the underlying DRBG in the resulting SecureRandom should
    // reseed on each request for bytes
    // additionalInput     initial additional input to be used for generating the initial continuous
    // health check block by the DRBG.
    return FipsDRBG.SHA512
        .fromEntropySource(entropySource, predictionResistant)
        .setPersonalizationString(personalizationString)
        .build(
            entropySource.generateSeed(DEFAULT_SECURITY_STRENGTH_BYTES / 2), // 16
            predictionResistant, // Cannot turn on predictionResistant due to performance (BoxJCA
            // parity)
            additionalInput);
  }

  /**
   * Generate numBytes of random bytes from the random source
   *
   * @param numBytes - number of bytes to generate
   * @return Array[Byte]
   */
  public byte[] generateRandomBytes(int numBytes) {
    byte[] bytes = new byte[numBytes];
    secureRandom.nextBytes(bytes);
    return bytes;
  }

  @Override
  public SecureRandom secureRandom() {
    return secureRandom;
  }

  public Provider provider() {
    return provider;
  }

  @Override
  protected Cipher aesCipherInternal(String transform)
      throws NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException {
    return Cipher.getInstance(transform, BC_FIPS_JCE_PROVIDER_NAME);
  }
}
