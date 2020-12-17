package com.box.dataplatform.s3a;

import com.box.dataplatform.util.Conf;
import org.apache.hadoop.conf.Configuration;

public class HadoopConf extends Conf {
  private final Configuration hadoopConf;

  public HadoopConf(String namespace, Configuration hadoopConf) {
    super(namespace);
    this.hadoopConf = hadoopConf;
  }

  public HadoopConf(Configuration hadoopConf) {
    super();
    this.hadoopConf = hadoopConf;
  }

  public static HadoopConf of(Configuration hadoopConf) {
    return new HadoopConf(hadoopConf);
  }

  public static HadoopConf of(String namespace, Configuration hadoopConf) {
    return new HadoopConf(namespace, hadoopConf);
  }

  @Override
  protected String get(String key) {
    return hadoopConf.get(key);
  }

  @Override
  protected void set(String key, String value) {
    hadoopConf.set(key, value);
  }
}
