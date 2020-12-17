package org.apache.iceberg;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDedupingIterable {
  int N;
  List<Integer> first100;
  List<Integer> duplicate;
  CloseableIterable<Integer> ogIterable;
  DedupingIterable<Integer, String> dedupingIterable;

  @Before
  public void setup() {
    N = 100;
    first100 = IntStream.range(0, N).boxed().collect(Collectors.toList());
    duplicate = new ArrayList<>(first100);
    duplicate.addAll(first100);
    ogIterable = CloseableIterable.withNoopClose(duplicate);
    dedupingIterable = new DedupingIterable<>(ogIterable, i -> i.toString());
  }

  @Test
  public void testDedupe() {
    List<Integer> output =
        StreamSupport.stream(dedupingIterable.spliterator(), false).collect(Collectors.toList());
    Assert.assertEquals(first100.size(), output.size());
    Assert.assertEquals(new HashSet<Integer>(output), new HashSet<Integer>(first100));
  }
}
