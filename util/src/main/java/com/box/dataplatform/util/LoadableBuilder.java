package com.box.dataplatform.util;

/**
 * TODO This doesn't actually do anything, consider removing it. Necessary for getting past the
 * Hadoop Configuration / Spark Options / Iceberg Options configuration barriers.
 */
public interface LoadableBuilder<T extends LoadableBuilder<T, K>, K> {
  /**
   * Method name for our custom {@link com.box.dataplatform.util.Conf}
   *
   * <p>Should load config from conf
   *
   * @param conf
   */
  T load(Conf conf);

  /**
   * Method name for our custom {@link com.box.dataplatform.util.Conf}
   *
   * <p>Should load config from conf
   *
   * @param conf
   */
  K build();
}
