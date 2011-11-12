/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api;

/**
 * Exception thrown when an operation does not complete within a specified deadline.
 */
public class ScalyrDeadlineException extends ScalyrException {
  /**
   * @param deadlineInMs Time allowed for the operation (in milliseconds).
   */
  public ScalyrDeadlineException(long deadlineInMs) {
    this("Operation", deadlineInMs);
  }
  
  /**
   * @param operationName Name of the operation that exceeded its deadline.
   * @param deadlineInMs Time allowed for the operation (in milliseconds).
   */
  public ScalyrDeadlineException(String operationName, long deadlineInMs) {
    super(operationName + " did not complete within the specified deadline of " + deadlineInMs + " milliseconds");
  }
}
