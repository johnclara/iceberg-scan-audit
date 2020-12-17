package com.box.dataplatform.iceberg.crypto;

import com.box.dataplatform.crypto.BCEncryptor;
import com.box.dataplatform.crypto.Encryptor;
import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.iceberg.encryption.crypto.AesGcmFormat;
import com.box.dataplatform.iceberg.encryption.crypto.DecryptingSeekableInputStream;
import com.box.dataplatform.iceberg.encryption.crypto.EncryptingPositionOutputStream;
import com.box.dataplatform.iceberg.encryption.dekprovider.PlaintextDekProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.Assert;
import org.junit.Test;

public class TestDecryptingInputStream {

  private final PlaintextDekProvider dekProvider;
  private final Encryptor cipherFactory;

  public TestDecryptingInputStream() {
    dekProvider = new PlaintextDekProvider();
    cipherFactory = BCEncryptor.INSTANCE;
  }

  byte[] createClear(int N) {
    return BCEncryptor.INSTANCE.generateRandomBytes(N);
  }

  byte[] createEncrypted(Dek dek, byte[] clear) throws IOException {
    ByteArrayOutputStream rawOutputStream =
        new ByteArrayOutputStream(clear.length + AesGcmFormat.AUTH_TAG_LENGTH);
    PositionOutputStream positionEncryptedStream = new TestPositionOutputStream(rawOutputStream);
    PositionOutputStream encryptedCipherStream =
        new EncryptingPositionOutputStream(
            positionEncryptedStream, dek.plaintextDek(), BCEncryptor.INSTANCE);
    encryptedCipherStream.write(clear);
    encryptedCipherStream.flush();
    encryptedCipherStream.close();
    byte[] encryptedOutput = rawOutputStream.toByteArray();
    return encryptedOutput;
  }

  @Test
  public void testSanity() throws IOException {
    int N = 2000;
    byte[] clear = createClear(N);
    Assert.assertEquals(N, clear.length);

    Dek dek =
        dekProvider.getNewDek(
            PlaintextDekProvider.PlaintextKekId.INSTANCE, AesGcmFormat.DEK_LENGTH, 0);
    byte[] encrypted = createEncrypted(dek, clear);
    Assert.assertEquals(N + AesGcmFormat.AUTH_TAG_LENGTH, encrypted.length);

    DecryptingSeekableInputStream clearInputStream =
        new DecryptingSeekableInputStream(
            new TestSeekableStream(() -> new ByteArrayInputStream(encrypted)),
            Long.valueOf(encrypted.length),
            dek.plaintextDek(),
            BCEncryptor.INSTANCE);
    byte[] inputBuffer = new byte[(int) N];
    int start = 0;
    while (start < N) {
      int amountRead = clearInputStream.read(inputBuffer, start, (int) N - start);
      if (amountRead <= -1) {
        throw new IllegalStateException("Didn't read the full stream");
      }
      start += amountRead;
    }
    Assert.assertEquals(-1, clearInputStream.read());
    Assert.assertArrayEquals(clear, inputBuffer);
    clearInputStream.close();
  }

  @Test
  public void testFromOffset() throws IOException {
    int N = 2000;
    byte[] clear = createClear(N);
    Assert.assertEquals(N, clear.length);

    Dek dek =
        dekProvider.getNewDek(
            PlaintextDekProvider.PlaintextKekId.INSTANCE, AesGcmFormat.DEK_LENGTH, 0);
    byte[] encrypted = createEncrypted(dek, clear);
    Assert.assertEquals(N + AesGcmFormat.AUTH_TAG_LENGTH, encrypted.length);

    int start = 600;

    int finalStart = start;

    DecryptingSeekableInputStream clearInputStream =
        new DecryptingSeekableInputStream(
            new TestSeekableStream(() -> new ByteArrayInputStream(encrypted)),
            Long.valueOf(encrypted.length),
            dek.plaintextDek(),
            cipherFactory);

    clearInputStream.seek(finalStart);

    byte[] unencrypted = new byte[(int) N];

    Arrays.fill(clear, 0, start, (byte) 0);

    while (start < N) {
      int amountRead = clearInputStream.read(unencrypted, start, (int) N - start);
      if (amountRead <= -1) {
        throw new IllegalStateException("Didn't read the full stream. Only read " + start);
      }
      start += amountRead;
    }

    Assert.assertEquals(-1, clearInputStream.read());

    Assert.assertArrayEquals(clear, unencrypted);
    clearInputStream.close();
  }
}
