package com.box.dataplatform.iceberg.core;

import static com.box.dataplatform.iceberg.core.DPCatalogConfig.*;

import com.box.dataplatform.iceberg.AbstractDPCatalog;
import com.box.dataplatform.iceberg.core.encryption.dekprovider.KmsDekProvider;
import com.box.dataplatform.iceberg.core.io.S3aDPFileIO;
import com.box.dataplatform.iceberg.core.metastore.DynamoMetastore;
import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.iceberg.encryption.crypto.AesGcmFormat;
import com.box.dataplatform.iceberg.encryption.crypto.SymmetricKeyCryptoFormat;
import com.box.dataplatform.iceberg.encryption.dekprovider.*;
import com.box.dataplatform.iceberg.io.DPFileIO;
import com.box.dataplatform.iceberg.metastore.Metastore;
import com.box.dataplatform.iceberg.util.JsonMapSerde;
import com.box.dataplatform.util.NestedConf;
import com.box.dataplatform.util.Properties;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class DPCatalog extends AbstractDPCatalog {
  public DPCatalog(
      Metastore metastore,
      DPFileIO dpFileIO,
      DelegatingDekProvider delegatingDekProvider,
      DelegatingDekProvider.DelegateKekId<? extends KekId> encryptedKekId,
      DelegatingDekProvider.DelegateKekId<? extends KekId> plaintextKekId,
      SymmetricKeyCryptoFormat cryptoFormat) {
    super(
        metastore,
        dpFileIO,
        delegatingDekProvider,
        encryptedKekId,
        plaintextKekId,
        cryptoFormat,
        JsonMapSerde.INSTANCE);
  }

  public static AbstractDPCatalog load(Map<String, String> properties, Configuration hadoopConf) {
    Properties conf = Properties.of(properties);

    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    Map<String, DekProvider<? extends KekId>> dekProviders = new HashMap<>();
    KmsDekProvider kmsDekProvider = KmsDekProvider.load(conf);
    dekProviders.put(AbstractKmsDekProvider.NAME, KmsDekProvider.load(conf));
    dekProviders.put(PlaintextDekProvider.NAME, new PlaintextDekProvider());

    String defaultDekProviderName =
        conf.propertyAsString(DEFAULT_DEK_PROVIDER_NAME, DEFAULT_DEK_PROVIDER_NAME_DEFAULT);
    DelegatingDekProvider delegatingDekProvider =
        new DelegatingDekProvider(defaultDekProviderName, dekProviders);

    DelegatingDekProvider.DelegateKekId<? extends KekId> encryptedKekId =
        delegatingDekProvider.loadKekId(NestedConf.of(conf, ENCRYPTED_KEKID_BASE));

    DelegatingDekProvider.DelegateKekId<? extends KekId> plaintextKekId =
        delegatingDekProvider.loadKekId(NestedConf.of(conf, PLAINTEXT_KEKID_BASE));

    SymmetricKeyCryptoFormat cryptoFormat = AesGcmFormat.load(conf);

    Metastore metastore = DynamoMetastore.load(conf);

    DPFileIO dpFileIO = new S3aDPFileIO(hadoopConf);
    dpFileIO.initialize(properties);

    return new DPCatalog(
        metastore, dpFileIO, delegatingDekProvider, encryptedKekId, plaintextKekId, cryptoFormat);
  }
}
