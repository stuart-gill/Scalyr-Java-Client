/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api;

/**
 * Exception thrown for low-level errors (e.g. network errors, internal server failures)
 * while communicating with the Scalyr server. Such errors do not indicate a problem with
 * the request (such as insufficient permissions), and are typically retriable.
 */
public class ScalyrNetworkException extends ScalyrException {
  public ScalyrNetworkException(String message) {
    super(message);
  }
  
  public ScalyrNetworkException(String message, Throwable cause) {
    super(message, cause);
  }
}
