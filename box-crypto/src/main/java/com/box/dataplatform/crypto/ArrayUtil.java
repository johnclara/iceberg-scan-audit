package com.box.dataplatform.crypto;

import java.util.Arrays;

/** All methods should not mutate arrays */
public class ArrayUtil {

  protected static byte[] drop(int n, byte[] src) {
    return Arrays.copyOfRange(src, n, src.length);
  }

  protected static byte[] padLeft(int n, byte[] src) {
    byte[] padArray = new byte[n + src.length];
    System.arraycopy(src, 0, padArray, n, src.length);
    return padArray;
  }

  protected static byte[] leftSplit(int n, byte[] src) {
    return Arrays.copyOfRange(src, 0, n);
  }

  protected static byte[] rightSplit(int n, byte[] src) {
    return Arrays.copyOfRange(src, n, src.length);
  }

  protected static byte[] concat(byte[] left, byte[] right) {
    byte[] both = new byte[left.length + right.length];
    System.arraycopy(left, 0, both, 0, left.length);
    System.arraycopy(right, 0, both, left.length, right.length);
    return both;
  }
}
