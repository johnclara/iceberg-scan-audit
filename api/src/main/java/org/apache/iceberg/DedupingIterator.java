package org.apache.iceberg;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Warning: This class is not thread safe
 *
 * @param <T> object type of the iterable
 * @param <O> object type of the key for deduping
 */
public class DedupingIterator<T, O> implements CloseableIterator<T> {
  private static Logger log = LoggerFactory.getLogger(DedupingIterator.class.getName());
  private final CloseableIterator<T> iterator;
  private final Function<T, O> getKey;
  private final Set<O> seen;
  private T buffer;
  private long start;

  public DedupingIterator(CloseableIterator<T> iterator, Function<T, O> getKey) {
    this.iterator = iterator;
    this.getKey = getKey;
    this.seen = new HashSet<>();
    buffer = null;
  }

  @Override
  public void close() throws IOException {
    log.info("Closing deduping iterator");
    iterator.close();
  }

  @Override
  public boolean hasNext() {
    return buffer != null || iterator.hasNext();
  }

  private void pump() {
    while (buffer == null && iterator.hasNext()) {
      T next = iterator.next();
      O key = getKey.apply(next);
      if (!seen.contains(key)) {
        seen.add(key);
        buffer = next;
      }
    }
    if (!iterator.hasNext()) {
      log.info("Iterator has completed");
      long end = System.currentTimeMillis();
      System.out.println("Deduping iterator took: " + (end - start));
    }
  }

  @Override
  public T next() {
    T output;
    if (buffer == null) {
      T next = iterator.next();
      O key = getKey.apply(next);
      seen.add(key);
      output = next;
      log.info("Outputting first object with key: " + key);
      start = System.currentTimeMillis();
    } else {
      output = buffer;
      buffer = null;
    }
    pump();
    return output;
  }
}
