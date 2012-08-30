/*
 * Scalyr client library
 * Copyright 2012 Scalyr, Inc.
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

package com.scalyr.api.internal;

import com.scalyr.api.LogHook;
import com.scalyr.api.logs.Severity;

/**
 * WARNING: this class, and all classes in the .internal package, should not be
 * used by client code. (This means you.) We reserve the right to make incompatible
 * changes to the .internal package at any time.
 */
public class Logging {
  /**
   * Hook which receives all internal messages logged by the Scalyr client. 
   */
  private static volatile LogHook hook = new LogHook.ThresholdLogger(Severity.info);
  
  /**
   * Specify the LogHook object to process internal messages logged by the Scalyr client.
   * Replaces any previous hook.
   */
  public static void setHook(LogHook value) {
    hook = value;
  }
  
  public static void log(Severity severity, String tag, String message) {
    log(severity, tag, message, null);
  }
  
  /**
   * Log a message regarding the internal functioning of the Scalyr client library.
   * 
   * @param severity Severity / importance of this message.
   * @param tag An invariant identifier for this message; taken from one of the string
   *     constants below.
   * @param message Human-readable message.
   * @param ex Exception associated with this message, or null.
   */
  public static void log(Severity severity, String tag, String message, Throwable ex) {
    hook.log(severity, tag, message, ex);
  }
  
  /**
   * Utility class used to limit log messages to a specified rate.
   */
  public static class LogLimiter {
    /**
     * Millisecond timestamp when this limiter last allowed a log event to go through,
     * or null if we never have.
     */
    private Long lastLogTimeMs = null;
    
    /**
     * Return true if it has been at least minIntervalMs since we last returned true.
     */
    public synchronized boolean allow(long minIntervalMs) {
      long nowMs = ScalyrUtil.currentTimeMillis();
      if (lastLogTimeMs == null || nowMs >= lastLogTimeMs + minIntervalMs) {
        lastLogTimeMs = nowMs;
        return true;
      } else {
        return false;
      }
    }
  }
  
  
  // These constants are used for the tag attribute to log().
  
  /**
   * Routine logging for communication with a Scalyr server.
   */
  public static final String tagServerCommunication = "server/communication";
  
  /**
   * Error communicating with a Scalyr server.
   */
  public static final String tagServerError = "server/error";
  
  /**
   * Backoff response from a Scalyr server.
   */
  public static final String tagServerBackoff = "server/error/backoff";
  
  /**
   * Error accessing a local configuration file.
   */
  public static final String tagLocalConfigFileError = "local/error/configFile";
  
  /**
   * I/O error accessing a local cache file.
   */
  public static final String tagKnobCacheIOError = "local/error/cacheFile";
  
  /**
   * Local cache file corrupt.
   */
  public static final String tagKnobCacheCorrupt = "local/error/cacheFile/corrupt";
  
  /**
   * Internal error in the client library.
   */
  public static final String tagInternalError = "local/error/internal";
  
  /**
   * Scalyr Logs client discarding events due to local buffer overflow.
   */
  public static final String tagLogBufferOverflow = "local/error/logBufferOverflow";
  
  /**
   * Knob file is not parseable as JSON.
   */
  public static final String tagKnobFileInvalid = "user/error/badKnobFile";
  
  /**
   * Exception escaped from Gauge.sample().
   */
  public static final String tagGaugeThrewException = "user/error/gaugeFailed";
  
  /**
   * Events.end() called with no matching start call.
   */
  public static final String tagMismatchedEnd = "user/error/mismatchedEnd";
}
