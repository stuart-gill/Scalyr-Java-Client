/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api;

/**
 * A generic interface for callback methods accepting a single parameter.
 */
public abstract class Callback<T> {
  /**
   * Invoke the callback with the given parameter.
   */
  public abstract void run(T value);
}
