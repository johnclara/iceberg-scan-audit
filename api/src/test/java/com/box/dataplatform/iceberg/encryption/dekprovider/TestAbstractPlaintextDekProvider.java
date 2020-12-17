package com.box.dataplatform.iceberg.encryption.dekprovider;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.util.Properties;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class TestAbstractPlaintextDekProvider {
  AbstractPlaintextDekProvider abstractPlaintextDekProvider;
  AbstractPlaintextDekProvider.PlaintextKekId kekId;

  @Before
  public void setup() {
    abstractPlaintextDekProvider = new MockPlaintextDekProvider();
    kekId = AbstractPlaintextDekProvider.PlaintextKekId.INSTANCE;
  }

  @Test
  public void testDumpLoadKekId() {
    Map<String, String> map = new HashMap<>();
    Properties props = Properties.of("", map);
    AbstractPlaintextDekProvider.PlaintextKekId.INSTANCE.dump(props);
    AbstractPlaintextDekProvider.PlaintextKekId plaintextKekId =
        abstractPlaintextDekProvider.loadKekId(props);

    assertEquals(kekId, plaintextKekId);
  }

  @Test
  public void testGetNewDek() {
    Dek dek = abstractPlaintextDekProvider.getNewDek(kekId, 3, 3);

    assertArrayEquals(MockPlaintextDekProvider.BYTES, dek.encryptedDek());
    assertArrayEquals(MockPlaintextDekProvider.BYTES, dek.plaintextDek());
    assertArrayEquals(AbstractPlaintextDekProvider.DUMMY_IV, dek.iv());
  }

  @Test
  public void testGetPlaintextDek() {
    byte[] bytes = new byte[4];

    Dek emptyDek = Dek.builder().setEncryptedDek(bytes).setIv(bytes).build();

    Dek dek = abstractPlaintextDekProvider.getPlaintextDek(kekId, emptyDek);

    assertArrayEquals(bytes, dek.encryptedDek());
    assertArrayEquals(bytes, dek.plaintextDek());
    assertArrayEquals(bytes, dek.iv());
  }
}
