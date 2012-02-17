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

package com.scalyr.api.logs;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.scalyr.api.internal.Logging;
import com.scalyr.api.internal.ScalyrUtil;

public class Events {
  private static AtomicReference<EventUploader> uploaderInstance = new AtomicReference<EventUploader>();
  
  /**
   * Initialize the Events reporting system. If this method has already been called, subsequent calls
   * are ignored.
   * 
   * @param apiToken The API authorization token to use when communicating with the Scalyr Logs server.
   * @param memoryLimit If not null, then we limit memory usage (for buffering events to be uploaded)
   *     to approximately this many bytes.
   */
  public static synchronized void init(String apiToken, Integer memoryLimit) {
    init(apiToken, memoryLimit, null);
  }
  
  public static synchronized void init(String apiToken, Integer memoryLimit, String serverAddress) {
    if (uploaderInstance.get() != null)
      return;
    
    LogService logService = new LogService(apiToken);
    if (serverAddress != null)
      logService.setServerAddress(serverAddress);
    
    uploaderInstance.set(new EventUploader(logService, memoryLimit,
        "sess_" + UUID.randomUUID(), true));
  }
  
  public static void finest(EventAttributes values) {
    event(Severity.finest, values);
  }
  
  public static void finer(EventAttributes values) {
    event(Severity.finer, values);
  }
  
  public static void fine(EventAttributes values) {
    event(Severity.fine, values);
  }
  
  public static void info(EventAttributes values) {
    event(Severity.info, values);
  }
  
  public static void warning(EventAttributes values) {
    event(Severity.warning, values);
  }
  
  public static void error(EventAttributes values) {
    event(Severity.error, values);
  }
  
  public static void fatal(EventAttributes values) {
    event(Severity.fatal, values);
  }
  
  public static Span startFinest(EventAttributes values) {
    return start(Severity.finest, values);
  }
  
  public static Span startFiner(EventAttributes values) {
    return start(Severity.finer, values);
  }
  
  public static Span startFine(EventAttributes values) {
    return start(Severity.fine, values);
  }
  
  public static Span startInfo(EventAttributes values) {
    return start(Severity.info, values);
  }
  
  public static Span startWarning(EventAttributes values) {
    return start(Severity.warning, values);
  }
  
  public static Span startError(EventAttributes values) {
    return start(Severity.error, values);
  }
  
  public static Span startFatal(EventAttributes values) {
    return start(Severity.fatal, values);
  }
  
  public static void event(Severity severity, EventAttributes attributes) {
    try {
      EventUploader instance = uploaderInstance.get();
      if (instance != null)
        instance.threadEvents.get().event(severity, attributes);
    } catch (Exception ex) {
      Logging.warn("Caught exception in com.scalyr.api.logs.Events.event()", ex);
    }
  }
  
  public static Span start(Severity severity, EventAttributes attributes) {
    try {
      EventUploader instance = uploaderInstance.get();
      if (instance != null)
        return instance.threadEvents.get().start(severity, attributes);
      else
        return new Span(ScalyrUtil.nanoTime(), severity);
    } catch (Exception ex) {
      Logging.warn("Caught exception in com.scalyr.api.logs.Events.start()", ex);
      
      return new Span(ScalyrUtil.nanoTime(), severity);
    }
  }
  
  public static void end(Span span) {
    end(span, null);
  }
  
  public static void end(Span span, EventAttributes attributes) {
    try {
      EventUploader instance = uploaderInstance.get();
      if (instance != null)
        instance.threadEvents.get().end(span, attributes);
    } catch (Exception ex) {
      Logging.warn("Caught exception in com.scalyr.api.logs.Events.end()", ex);
    }
  }
  
  /**
   * Force all events recorded to date to be uploaded to the server.
   */
  public static synchronized void flush() {
    EventUploader instance = uploaderInstance.get();
    if (instance != null)
      instance.flush();
  }
  
  /**
   * Wipe the state of the Events reporting system. Should only be used for internal tests.
   */
  public static synchronized void _reset(String artificialSessionId,
      LogService logService, Integer memoryLimit, boolean autoUpload) {
    if (uploaderInstance.get() != null)
      uploaderInstance.get().terminate();
    
    uploaderInstance.set(new EventUploader(logService, memoryLimit, artificialSessionId, autoUpload));
  }
  
  /**
   * Only for use by internal tests.
   */
  public static void _uploadTick() {
    uploaderInstance.get().uploadTimerTick();
  }
}
