package org.apache.iceberg.io;

import java.util.Iterator;

public class CloseableIterableNoCheck {
  public static <E> CloseableIterable<E> concat(Iterable<CloseableIterable<E>> iterable) {
    Iterator<CloseableIterable<E>> iterables = iterable.iterator();
    return new CloseableIterable.ConcatCloseableIterable<>(iterable);
  }
}
