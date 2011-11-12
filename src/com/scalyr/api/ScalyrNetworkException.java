/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api;

/**
 * Exception thrown for network-level errors while communicating with the Scalyr server.
 */
public class ScalyrNetworkException extends ScalyrException {
  public ScalyrNetworkException(String message) {
    super(message);
  }
  
  public ScalyrNetworkException(String message, Throwable cause) {
    super(message, cause);
  }
}
