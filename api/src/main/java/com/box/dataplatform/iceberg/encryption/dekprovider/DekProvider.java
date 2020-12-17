package com.box.dataplatform.iceberg.encryption.dekprovider;

import com.box.dataplatform.iceberg.encryption.Dek;
import com.box.dataplatform.iceberg.encryption.KekId;
import com.box.dataplatform.util.Conf;
import java.io.Serializable;

public abstract class DekProvider<K extends KekId> implements Serializable {
  public abstract Dek getNewDek(K kekId, int dekLength, int ivLength);

  public abstract Dek getPlaintextDek(K kekId, Dek dek);

  public abstract K loadKekId(Conf conf);
}
