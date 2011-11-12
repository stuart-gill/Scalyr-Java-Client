/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api;

/**
 * Base class for all exceptions thrown by the Scalyr client library.
 */
public class ScalyrException extends RuntimeException {
  public ScalyrException(String message) {
    super(message);
  }
  
  public ScalyrException(String message, Throwable cause) {
    super(message, cause);
  }
}
