package org.apache.iceberg.addons.cataloglite;

import java.io.Serializable;

class Lockable<T extends Serializable> implements Serializable {
  private final boolean locked;
  private final T value;

  public static <T extends Serializable> Lockable<T> of(T value) {
    return new Lockable<>(false, value);
  }

  protected Lockable(boolean locked, T value) {
    this.locked = locked;
    this.value = value;
  }

  public Lockable<T> set(T otherValue) {
    if (locked && !value.equals(otherValue)) {
      throw new IllegalArgumentException("Field set and cannot be updated");
    } else {
      return new Lockable<>(locked, otherValue);
    }
  }

  public Lockable<T> lock() {
    return new Lockable<>(true, value);
  }

  public T get() {
    return value;
  }
}

