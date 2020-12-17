package com.box.dataplatform.util;

/**
 * TODO: This interface doesn't actually do anything, it just says that this method should exist.
 *
 * <p>Could be removed.
 */
public interface Dumpable {
  /**
   * Method name for our custom {@link com.box.dataplatform.util.Conf}
   *
   * <p>Should add current config to conf
   *
   * @param conf
   */
  void dump(Conf conf);
}
