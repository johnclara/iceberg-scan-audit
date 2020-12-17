package com.box.dataplatform.iceberg.encryption.dekprovider;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.util.Properties;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TestAbstractKmsDekProvider {
  @Mock MockKms mockKms;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  AbstractKmsDekProvider abstractKmsDekProvider;
  AbstractKmsDekProvider.KmsKekId kekId;
  String keyId;

  @Before
  public void setup() {
    abstractKmsDekProvider = new MockKmsDekProvider(mockKms);
    keyId = "myKey";
    kekId = new AbstractKmsDekProvider.KmsKekId(keyId);
  }

  @Test
  public void testLoadKekId() {
    Map<String, String> map = new HashMap<>();
    Properties props = Properties.of("", map);
    props.setString(AbstractKmsDekProvider.KEK_ID, keyId);
    AbstractKmsDekProvider.KmsKekId kmsKekId = abstractKmsDekProvider.loadKekId(props);

    assertEquals(kekId, kmsKekId);
    verifyNoMoreInteractions(mockKms);
  }

  @Test
  public void testDumpLoadKekId() {
    Map<String, String> map = new HashMap<>();
    Properties props = Properties.of("", map);
    kekId.dump(props);
    AbstractKmsDekProvider.KmsKekId kmsKekId = abstractKmsDekProvider.loadKekId(props);

    assertEquals(kekId, kmsKekId);
    verifyNoMoreInteractions(mockKms);
  }

  @Test
  public void testGetNewDek() {
    byte[] bytes = new byte[3];

    when(mockKms.getPlaintextFromKms(any(), anyInt()))
        .thenThrow(new RuntimeException())
        .thenReturn(new AbstractKmsDekProvider.KmsGenerateDekResponse(bytes, bytes));

    Dek dek = abstractKmsDekProvider.getNewDek(kekId, 3, 3);

    assertArrayEquals(bytes, dek.encryptedDek());
    assertArrayEquals(bytes, dek.plaintextDek());
    assertArrayEquals(AbstractKmsDekProvider.DUMMY_IV, dek.iv());
    verify(mockKms, times(2)).getPlaintextFromKms(any(), anyInt());
  }

  @Test
  public void testGetPlaintextDek() {
    byte[] bytes = new byte[3];

    when(mockKms.getPlaintextFromKms(any(), any()))
        .thenThrow(new RuntimeException())
        .thenReturn(new AbstractKmsDekProvider.KmsDecryptResponse(bytes));

    Dek emptyDek =
        Dek.builder().setEncryptedDek(bytes).setIv(AbstractKmsDekProvider.DUMMY_IV).build();

    Dek dek = abstractKmsDekProvider.getPlaintextDek(kekId, emptyDek);

    assertArrayEquals(bytes, dek.encryptedDek());
    assertArrayEquals(bytes, dek.plaintextDek());
    assertArrayEquals(AbstractKmsDekProvider.DUMMY_IV, dek.iv());
    verify(mockKms, times(2)).getPlaintextFromKms(any(), any());
  }
}
