package com.scalyr.api.internal;

/**
 * This class wraps Thread.sleep. We use it to allow an alternate implementation
 * to be substituted during tests.
 */
public abstract class Sleeper {
  public abstract void sleep(int intervalInMs);
  
  /**
   * Standard Sleeper implementation. Overridden in tests.
   */
  public static Sleeper instance;
  
  static {
    instance = new Sleeper() {
    @Override public void sleep(int intervalInMs) {
      try {
        Thread.sleep(intervalInMs);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  };
  }
}
