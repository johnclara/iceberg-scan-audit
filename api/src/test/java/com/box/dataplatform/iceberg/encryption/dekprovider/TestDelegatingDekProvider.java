package com.box.dataplatform.iceberg.encryption.dekprovider;

import static com.box.dataplatform.test.util.SerializableUtil.deserialize;
import static com.box.dataplatform.test.util.SerializableUtil.serialize;
import static org.junit.Assert.assertEquals;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.util.Properties;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class TestDelegatingDekProvider {
  Map<String, DekProvider<? extends KekId>> providers;
  Mock1DekProvider mock1DekProvider;
  Mock2DekProvider mock2DekProvider;
  DelegatingDekProvider delegatingDekProvider;
  String mock1ProviderName;
  String mock2ProviderName;

  Mock1DekProvider.Mock1KekId mock1KekId;
  DelegatingDekProvider.DelegateKekId<Mock1DekProvider.Mock1KekId> delegate1KekId;

  Mock2DekProvider.Mock2KekId mock2KekId;
  DelegatingDekProvider.DelegateKekId<Mock2DekProvider.Mock2KekId> delegate2KekId;

  @Before
  public void setup() {
    mock1ProviderName = "mock1";
    mock2ProviderName = "mock2";
    providers = new HashMap<>();
    mock1DekProvider = new Mock1DekProvider();
    mock2DekProvider = new Mock2DekProvider();
    providers.put(mock1ProviderName, mock1DekProvider);
    providers.put(mock2ProviderName, mock2DekProvider);
    delegatingDekProvider = new DelegatingDekProvider(mock1ProviderName, providers);

    mock1KekId = new Mock1DekProvider.Mock1KekId("one", 2);
    delegate1KekId =
        new DelegatingDekProvider.DelegateKekId<Mock1DekProvider.Mock1KekId>(
            mock1ProviderName, mock1DekProvider, mock1KekId);

    mock2KekId = new Mock2DekProvider.Mock2KekId(3, 4.0);
    delegate2KekId =
        new DelegatingDekProvider.DelegateKekId<Mock2DekProvider.Mock2KekId>(
            mock2ProviderName, mock2DekProvider, mock2KekId);
  }

  /**
   * This test checks that when a new dek is created it will get created by the delegate dek
   * provider.
   *
   * <p>Mock 1 will always create deks and ivs with 2 bytes.
   *
   * <p>Mock 2 will always creae deks and ivs with 1 byte.
   */
  @Test
  public void testGetNewDek() {
    Dek dek1 = delegatingDekProvider.getNewDek(delegate1KekId, 0, 0);
    assertEquals(Mock1DekProvider.BYTES, dek1.encryptedDek());

    Dek dek2 = delegatingDekProvider.getNewDek(delegate2KekId, 0, 0);
    assertEquals(Mock2DekProvider.BYTES, dek2.encryptedDek());
  }

  @Test
  public void testGetPlaintextDek() {
    Dek emptyDek1 = Dek.builder().setEncryptedDek(new byte[0]).setIv(new byte[0]).build();
    Dek emptyDek2 = Dek.builder().setEncryptedDek(new byte[0]).setIv(new byte[0]).build();

    Dek dek1 = delegatingDekProvider.getPlaintextDek(delegate1KekId, emptyDek1);
    assertEquals(Mock1DekProvider.BYTES, dek1.plaintextDek());

    Dek dek2 = delegatingDekProvider.getPlaintextDek(delegate2KekId, emptyDek2);
    assertEquals(Mock2DekProvider.BYTES, dek2.plaintextDek());
  }

  @Test
  public void testDelegateKekIdDumpLoad() {
    Map<String, String> map = new HashMap<>();
    Properties conf = Properties.of(map);
    delegate1KekId.dump(conf);
    DelegatingDekProvider.DelegateKekId<? extends KekId> loadedKekId =
        delegatingDekProvider.loadKekId(conf);
    assertEquals(loadedKekId, delegate1KekId);
  }

  @Test
  public void testDelegateKekIdSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    Serializable uut = delegate1KekId;
    byte[] serialized1 = serialize(uut);
    byte[] serialized2 = serialize(uut);

    Object deserialized1 = deserialize(serialized1);
    Object deserialized2 = deserialize(serialized2);
    assertEquals(deserialized1, deserialized2);
    assertEquals(uut, deserialized1);
    assertEquals(uut, deserialized2);
  }

  @Test
  public void testSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    Serializable uut = delegatingDekProvider;
    byte[] serialized1 = serialize(uut);
    byte[] serialized2 = serialize(uut);

    Object deserialized1 = deserialize(serialized1);
    Object deserialized2 = deserialize(serialized2);
    assertEquals(deserialized1, deserialized2);
    assertEquals(uut, deserialized1);
    assertEquals(uut, deserialized2);
  }
}
