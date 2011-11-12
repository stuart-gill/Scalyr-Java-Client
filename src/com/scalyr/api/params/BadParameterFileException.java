/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api.params;

import com.scalyr.api.ScalyrException;

/**
 * Exception thrown when a parameter file contains invalid content (e.g. is expected to contain
 * JSON data but is not a valid JSON file).
 */
public class BadParameterFileException extends ScalyrException {
  public BadParameterFileException(String message) {
    super(message);
  }
  
  public BadParameterFileException(String message, Throwable cause) {
    super(message, cause);
  }
  
  /**
   * Construct a clone of the specified exception. Used to generate an exception object with
   * a fresh stack trace, but the same message and cause.
   */
  public BadParameterFileException(BadParameterFileException valueToClone) {
    super(valueToClone.getMessage(), valueToClone.getCause());
  }
}
