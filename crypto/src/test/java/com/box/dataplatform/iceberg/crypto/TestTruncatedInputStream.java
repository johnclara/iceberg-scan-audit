package com.box.dataplatform.iceberg.crypto;

import com.box.dataplatform.iceberg.encryption.crypto.TruncatedInputStream;
import com.google.common.base.Strings;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class TestTruncatedInputStream {
  String source = Strings.repeat("8", 15);
  ByteArrayInputStream inputStream =
      new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8));
  TruncatedInputStream truncatedInputStream = new TruncatedInputStream(inputStream, 10L);

  String toString(InputStream is) {
    return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));
  }

  @Test
  public void testSanity() {
    String text = toString(truncatedInputStream);
    Assert.assertEquals(Strings.repeat("8", 10), text);
  }

  @Test
  public void testReadAByte() throws IOException {
    char result = (char) truncatedInputStream.read();
    Assert.assertEquals('8', result);
  }

  @Test
  public void testReadIntoBuffer() throws IOException {
    byte[] expectedResult = new byte[15];
    Arrays.fill(expectedResult, 0, 10, (byte) '8');

    byte[] buffer = new byte[15];

    int numRead = truncatedInputStream.read(buffer);
    Assert.assertEquals(10, numRead);
    Assert.assertArrayEquals(expectedResult, buffer);
  }

  @Test
  public void testReadIntoPartOfABuffer() throws IOException {
    byte[] expectedResult = new byte[15];
    Arrays.fill(expectedResult, 2, 12, (byte) '8');

    byte[] buffer = new byte[15];

    int numRead = truncatedInputStream.read(buffer, 2, 13);
    Assert.assertEquals(10, numRead);
    Assert.assertArrayEquals(expectedResult, buffer);
  }

  @Test
  public void testReadOnceAndRest() throws IOException {
    Assert.assertEquals(truncatedInputStream.read(), (byte) '8');
    String result = toString(truncatedInputStream);
    Assert.assertEquals(Strings.repeat("8", 9), result);
    Assert.assertEquals(-1, truncatedInputStream.read());
  }
}
