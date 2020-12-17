package com.box.dataplatform.iceberg.encryption;

import static org.mockito.Mockito.*;

import com.box.dataplatform.util.Properties;
import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDek {
  byte[] encryptedDek;
  byte[] iv;
  Dek dek;

  @Before
  public void setup() {
    Random random = new Random();
    int numBytes = 32;
    encryptedDek = new byte[numBytes];
    iv = new byte[numBytes];
    random.nextBytes(encryptedDek);
    random.nextBytes(iv);
    dek = new Dek(encryptedDek, iv);
    encryptedDek = Arrays.copyOf(encryptedDek, encryptedDek.length);
    iv = Arrays.copyOf(iv, iv.length);
  }

  @Test
  public void testDumpLoad() {
    Map<String, String> map = new HashMap<>();
    Properties conf = Properties.of(map);

    dek.dump(conf);
    Assert.assertArrayEquals(dek.encryptedDek(), encryptedDek);
    Assert.assertArrayEquals(dek.iv(), iv);

    Dek loaded = Dek.load(conf);
    Assert.assertEquals(loaded, dek);
  }
}
