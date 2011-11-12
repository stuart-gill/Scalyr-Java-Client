/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api;

/**
 * Exception thrown for errors returned from the Scalyr server.
 */
public class ScalyrServerException extends ScalyrException {
  public ScalyrServerException(String message) {
    super(message);
  }
  
  public ScalyrServerException(String message, Throwable cause) {
    super(message, cause);
  }
}
