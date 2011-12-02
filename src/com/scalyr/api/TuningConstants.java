package com.scalyr.api;

/**
 * Tunable parameters.
 */
public class TuningConstants {
  /**
   * Time span during which API operations may be retried (e.g. in response to a network
   * error). Once this many milliseconds have elapsed from the initial API invocation, we
   * no longer issue retries.
   */
  public static final int MAXIMUM_RETRY_PERIOD_MS = 60000;
  
  /**
   * Maximum time (in milliseconds) for opening an HTTP connection to the Scalyr server.
   * If this time is exceeded, we consider server invocation to have failed.
   */
  public static final int HTTP_CONNECT_TIMEOUT_MS = 10000;
}
