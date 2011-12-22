/*
 * Scalyr client library
 * Copyright 2011 Scalyr, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  
  /**
   * Time (in milliseconds) from when HostedConfigurationFile completes fetching a file from
   * the server, until it issues the next request. If the fetch failed, subsequent requests
   * use a longer interval, up to MAXIMUM_FETCH_INTERVAL.
   */
  public static final int MINIMUM_FETCH_INTERVAL = 500;
  
  /**
   * Minimum interval used when the previous request was unsuccessful.
   */
  public static final int MINIMUM_FETCH_INTERVAL_AFTER_ERROR = 5000;
  
  public static final int MAXIMUM_FETCH_INTERVAL = 60000;
}
