package com.scalyr.api.logs;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Encapsulates a double and allows atomic incrementing. Similar to AtomicLong, though we don't bother
 * to implement the complete functionality.
 */
public class AtomicDouble {
  private volatile long bits;
  
  private static final AtomicLongFieldUpdater<AtomicDouble> updater =
      AtomicLongFieldUpdater.newUpdater(AtomicDouble.class, "bits");
  
  public AtomicDouble() {
    this(0.0);
  }
  
  public AtomicDouble(double initialValue) {
    bits = Double.doubleToRawLongBits(initialValue);
  }
  
  public double get() {
    return Double.longBitsToDouble(bits);
  }
  
  public void set(double newValue) {
    bits = Double.doubleToRawLongBits(newValue);
  }
  
  public void add(double delta) {
    while (true) {
      long currentBits = bits;
      double currentValue = Double.longBitsToDouble(currentBits);
      long nextBits = Double.doubleToRawLongBits(currentValue + delta);
      if (updater.compareAndSet(this, currentBits, nextBits)) {
        break;
      }
    }
  }
}
