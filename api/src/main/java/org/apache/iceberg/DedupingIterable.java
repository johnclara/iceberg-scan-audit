package org.apache.iceberg;

import java.io.IOException;
import java.util.function.Function;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <T> iterable object type
 * @param <O> key for deduping
 */
public class DedupingIterable<T, O> implements CloseableIterable<T> {
  private static final Logger log = LoggerFactory.getLogger(DedupingIterable.class.getName());
  private final CloseableIterable<T> iterable;
  private final Function<T, O> getKey;

  public DedupingIterable(CloseableIterable<T> iterable, Function<T, O> getKey) {
    this.iterable = iterable;
    this.getKey = getKey;
  }

  @Override
  public CloseableIterator<T> iterator() {
    log.info("Creating deduping iterator");
    return new DedupingIterator<T, O>(iterable.iterator(), getKey);
  }

  @Override
  public void close() throws IOException {
    log.info("Closing deduping iterable");
    iterable.close();
  }
}
