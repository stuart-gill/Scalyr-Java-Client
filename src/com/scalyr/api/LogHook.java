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

package com.scalyr.api;

import java.util.Date;

import com.scalyr.api.internal.Logging;
import com.scalyr.api.logs.Severity;

/**
 * A LogHook receives all messages logged by the Scalyr client library. 
 */
public abstract class LogHook {
  /**
   * Log a message regarding the internal functioning of the Scalyr client library.
   * 
   * @param severity Severity / importance of this message.
   * @param tag An invariant identifier for this message.
   * @param message Human-readable message.
   * @param ex Exception associated with this message, or null.
   */
  public abstract void log(Severity severity, String tag, String message, Throwable ex);
  
  public static class ThresholdLogger extends LogHook {
    private final Severity minSeverity;
    
    public ThresholdLogger(Severity minSeverity) {
      this.minSeverity = minSeverity;
    }
    
    @Override public void log(Severity severity, String tag, String message, Throwable ex) {
      // Log all messages at or above minSeverity to stdout.
      if (severity.ordinal() >= minSeverity.ordinal()) {
        System.out.println(new Date() + ": " + tag + " (" + message + ")");
        if (ex != null)
          ex.printStackTrace(System.out);
      }
    }
  }
  
  /**
   * Specify the LogHook object to process internal messages logged by the Scalyr client.
   * Replaces any previous hook.
   */
  public static void setHook(LogHook value) {
    Logging.setHook(value);
  }
}
