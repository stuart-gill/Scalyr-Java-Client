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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.scalyr.api.TuningConstants;
import com.scalyr.api.internal.Logging;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.api.json.JSONArray;
import com.scalyr.api.json.JSONObject;

/**
 * Internal class which buffers events, and periodically uploads them to the Scalyr Logs service.
 */
public class EventUploader {
  /**
   * Time when this uploader was created. Used as a proxy for the startup time of the
   * containing process.
   */
  private final long launchTimeNs = ScalyrUtil.nanoTime();
  
  /**
   * Unique ID for this process invocation.
   */
  private final String sessionId;
  
  /**
   * Client-specified attributes to associate with the events we upload, or null.
   */
  private final EventAttributes serverAttributes;
  
  /**
   * If true, then we automatically upload events using a timer. If false, then the client must
   * manually initiate upload. Always true except during tests.
   */
  private final boolean autoUpload;
  
  /**
   * IP address of this server.
   */
  private String ourIpAddress;
  
  /**
   * Hostname of this server.
   */
  private String ourHostname;
  
  /**
   * ThreadLocal for tracking information per thread -- thread ID, name, etc.
   */
  final ThreadLocal<PerThreadState> threadEvents = new ThreadLocal<PerThreadState>() {
    @Override protected PerThreadState initialValue() {
      Thread thread = Thread.currentThread();
      PerThreadState temp = new PerThreadState(thread.getId(), thread.getName());
      synchronized (threads) {
        assert(!threads.containsKey(thread));
        threads.put(thread, temp);
      }
      return temp;
    }
  };
  
  /**
   * All outstanding threads for which we have recorded at least one event.
   * 
   * TODO: should scavenge records for idle threads.
   */
  private Map<Thread, PerThreadState> threads = new HashMap<Thread, PerThreadState>();
  
  /**
   * Events which have been recorded since the last call to uploadBuffer.
   */
  private JSONArray pendingEvents = new JSONArray();
  
  /**
   * Estimated RAM usage for pendingEvents.
   */
  private long pendingEventsSize = 0;
  
  /**
   * True if we've discarded at least one event because we reached memoryLimit.
   * Reset whenever an upload completes (thus freeing up memory). This ensures
   * that we don't "stutter" at the edge of the memory boundary, yielding a
   * confusing situation where events are dropped intermittently.
   */
  private boolean pendingEventsReachedLimit = false;
  
  /**
   * If we have an in-flight upload request to the server, this holds the estimated
   * RAM usage of that request. Otherwise null.
   */
  private Long pendingUploadSize = null;
  
  /**
   * Timer used to generate upload events. Allocated when the first event is recorded.
   */
  private Timer uploadTimer = null;
  private TimerTask uploadTask = null;
  
  /**
   * Object used to synchronize access to pendingEvents, pendingEventsSize,
   * pendingEventsReachedLimit, and pendingUploadSize.
   */
  private final Object uploadSynchronizer = new Object(); 
  
  /**
   * Service we upload to.
   */
  private final LogService logService;
  
  /**
   * Limit If not null, then we limit memory usage (for buffering events to be uploaded)
   * to approximately this many bytes.
   */
  private final Integer memoryLimit;
  
  /**
   * Construct an EventUploader to buffer events and upload them to the given LogService instance.
   */
  EventUploader(LogService logService, Integer memoryLimit, String sessionId, boolean autoUpload,
      EventAttributes serverAttributes) {
    this.logService = logService;
    this.autoUpload = autoUpload;
    
    // We divide memoryLimit in half to be conservative, as the upload process makes a
    // copy of the events being uploaded.
    // TODO: fix this.
    this.memoryLimit = (memoryLimit != null) ? (memoryLimit / 2) : null;
    
    this.sessionId = sessionId;
    this.serverAttributes = serverAttributes;
    
    launchUploadTimer();
  }
  
  synchronized void terminate() {
    if (uploadTask != null)
      uploadTask.cancel();
    
    if (uploadTimer != null)
      uploadTimer.cancel();
  }
  
  /**
   * Force all events recorded to date to be uploaded to the server.
   */
  synchronized void flush() {
    uploadTimerTick();
  }
  
  /**
   * This method is called periodically by a timer. If we are not in the middle of uploading
   * a batch of events to the server, we snapshot the events currently buffered and begin
   * uploading them.
   */
  synchronized void uploadTimerTick() {
    JSONArray eventsToUpload;
    synchronized (uploadSynchronizer) {
      // If we have an upload request in flight, don't initiate another one now. 
      if (pendingUploadSize != null)
        return;
      
      eventsToUpload = pendingEvents;
      if (eventsToUpload.size() == 0)
        return; // nothing to upload
      
      pendingUploadSize = pendingEventsSize;
      
      pendingEvents = new JSONArray();
      pendingEventsSize = 0;
      pendingEventsReachedLimit = false;
    }
    
    List<PerThreadState> threadsSnapshot;
    synchronized (threads) {
      threadsSnapshot = new ArrayList<PerThreadState>(threads.values());
    }
    
    // Sort the threads alphabetically by name -- this ensures a stable order when
    // uploading, which is helpful for tests.
    Collections.sort(threadsSnapshot, new Comparator<PerThreadState>(){
      @Override public int compare(PerThreadState a, PerThreadState b) {
        if (a.name == null || b.name == null) {
          if (a.name == null && b.name == null)
            return 0;
          return (a.name == null) ? -1 : 1;
        }
        return a.name.compareTo(b.name);
      }});
    
    JSONObject sessionInfo = new JSONObject();
    sessionInfo.put("sessionId", sessionId);
    sessionInfo.put("launchTime", launchTimeNs);
    
    if (ourIpAddress != null)
      sessionInfo.put("ipAddress", ourIpAddress);
    
    if (ourHostname != null)
      sessionInfo.put("hostname", ourHostname);

    if (serverAttributes != null)
      for (Map.Entry<String, Object> entry : serverAttributes.values.entrySet())
        sessionInfo.put(entry.getKey(), entry.getValue());
    
    JSONArray threadInfos = new JSONArray();
    
    // TODO: only include threads for which we are uploading at least one event.
    for (PerThreadState thread : threadsSnapshot) {
      JSONObject threadInfo = new JSONObject();
      threadInfo.put("id", Long.toString(thread.threadId));
      threadInfo.put("name", thread.name);
      threadInfos.add(threadInfo);
    }
    
    try {
      logService.uploadEvents(sessionId, sessionInfo, eventsToUpload, threadInfos);
    } finally {
      synchronized (uploadSynchronizer) {
        pendingUploadSize = null;
      }
    }
  }
  
  private synchronized void launchUploadTimer() {
    if (uploadTimer == null) {
      ourHostname = ScalyrUtil.getHostname();
      ourIpAddress = ScalyrUtil.getIpAddress();
      
      uploadTimer = new Timer("EventUploader", true);
      if (autoUpload) {
        uploadTask = new TimerTask(){
          @Override public void run() {
            try {
              uploadTimerTick();
            } catch (Exception ex) {
              Logging.warn("Exception in Logs upload timer", ex);
            }
          }};
        uploadTimer.schedule(uploadTask, TuningConstants.EVENT_UPLOAD_INTERVAL_MS,
            TuningConstants.EVENT_UPLOAD_INTERVAL_MS);
      }
    }
  }
  
  /**
   * Information that we store per running thread.
   */
  class PerThreadState {
    /**
     * Thread.getId() for the thread whose events we store.
     */
    final long threadId;
    
    /**
     * Thread name.
     */
    final String name;
    
    /**
     * Time when the thread was launched.
     */
    final long startTime = getMonotonicNanos();
    
    PerThreadState(long threadId, String name) {
      this.threadId = threadId;
      this.name = name;
    }
    
    /**
     * Add a span-start event to the buffer, and return its timestamp.
     */
    Span start(Severity severity, EventAttributes attributes) {
      long timestamp = getMonotonicNanos();
      JSONObject eventJson = eventToJson(threadId, timestamp, LogService.SPAN_TYPE_START, severity,
          attributes);
      
      addEventToBuffer(eventJson);
      
      return new Span(timestamp, severity);
    }
    
    /**
     * Add an end-span event to the buffer.
     */
    void end(Span span, EventAttributes attributes) {
      JSONObject eventJson = eventToJson(threadId, getMonotonicNanos(), LogService.SPAN_TYPE_END,
          span.severity, attributes);
      eventJson.put("startTS", span.startTime);
      addEventToBuffer(eventJson);
    }

    /**
     * Add a non-span event to the buffer.
     */
    void event(Severity severity, EventAttributes attributes) {
      addEventToBuffer(eventToJson(threadId, getMonotonicNanos(), LogService.SPAN_TYPE_LEAF, severity, attributes));
    }
  }
  
  private void addEventToBuffer(JSONObject eventJson) {
    long eventSize = SizeEstimator.estimateSize(eventJson);
    
    synchronized (uploadSynchronizer) {
      long currentMemoryUsage = pendingEventsSize + (pendingUploadSize != null ? pendingUploadSize: 0);
      if (pendingEventsReachedLimit || (memoryLimit != null && currentMemoryUsage + eventSize > memoryLimit)) {
        Logging.warn("com.scalyr.api.logs: Discarding event, as buffer size of "
            + memoryLimit + " bytes has been reached.");
        pendingEventsReachedLimit = true;
        return;
      }
      
      pendingEvents.add(eventJson);
      pendingEventsSize += eventSize + 4;
    }
  }
  
  static JSONObject eventToJson(long threadId, long timestamp, int spanType, Severity severity, EventAttributes attributes) {
    JSONObject json = new JSONObject();
    
    // Note that we store the timestamp as a string, not a number. This is because some JSON packages
    // convert all numbers to floating point, and a 64-bit floating point value doesn't have sufficient
    // precision to represent a nanosecond timestamp. We take a similar precaution for the thread ID.
    json.put("thread", Long.toString(threadId));
    json.put("ts", Long.toString(timestamp));
    json.put("type", spanType);
    json.put("sev", severity.ordinal());
    
    if (attributes != null && attributes.values.size() > 0) {
      JSONObject jsonAttrs = new JSONObject();
      
      for (Map.Entry<String, Object> entry : attributes.values.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof String && ((String)value).length() > TuningConstants.MAXIMUM_EVENT_ATTRIBUTE_LENGTH) {
          value = ((String)value).substring(0, TuningConstants.MAXIMUM_EVENT_ATTRIBUTE_LENGTH - 3) + "...";
        }
        jsonAttrs.put(entry.getKey(), value);
      }
      
      json.put("attrs", jsonAttrs);
    }
    
    return json;
  }
  
  /**
   * Most recently assigned timestamp. Used to ensure that timestamps are strictly increasing (within
   * a given process): nanoTime() can sometimes run backwards.
   */
  private static long lastTimestamp = ScalyrUtil.nanoTime();
  
  /**
   * Object used to synchronize access to lastTimestamp.
   */
  private static final Object timestampLocker = new Object();
  
  /**
   * Return the current time (in nanoseconds since the epoch), but strictly greater than any
   * previous result from this method.
   */
  private static long getMonotonicNanos() {
    long now = ScalyrUtil.nanoTime();
    synchronized (timestampLocker) {
      // TODO: warn when nanoTime runs backwards.
      long timestamp = Math.max(lastTimestamp + 1, now);
      lastTimestamp = timestamp;
      
      return timestamp;
    }
  }
}
