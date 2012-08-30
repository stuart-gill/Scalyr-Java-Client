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

package com.scalyr.api.logs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.scalyr.api.TuningConstants;
import com.scalyr.api.internal.ChunkSizeList;
import com.scalyr.api.internal.CircularByteArray;
import com.scalyr.api.internal.Logging;
import com.scalyr.api.internal.Logging.LogLimiter;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.api.json.JSONArray;
import com.scalyr.api.json.JSONObject;
import com.scalyr.api.json.JSONParser;
import com.scalyr.api.json.ParseException;
import com.scalyr.api.json.RawJson;
import com.scalyr.api.logs.EventFilter.FilterInput;
import com.scalyr.api.logs.EventFilter.FilterOutput;

/**
 * Internal class which buffers events, and periodically uploads them to the Scalyr Logs service.
 */
public class EventUploader {
  /**
   * Empty attributes object. Should be considered immutable.
   */
  private static final EventAttributes emptyAttributes = new EventAttributes();
  
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
        ScalyrUtil.Assert(!threads.containsKey(thread), "collision in threads table");
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
   * Holds the serialized form of all events which have been recorded since the last call to uploadBuffer.
   * Each event is followed by a comma, so the contents of the buffer always look like this:
   * 
   *   {...},{...},{...}, ...
   */
  private final CircularByteArray pendingEventBuffer;
  
  /**
   * Defines a series of chunks in pendingEventBuffer. Each entry is a byte count, describing a
   * range of pendingEventBuffer which begins and ends at event boundaries (an "event boundary"
   * is the beginning of the buffer, or a position just after a comma). The sum of the entries
   * is always exactly equal to the number of bytes in pendingEventBuffer, and no entry is ever
   * larger than TuningConstants.MAX_EVENT_UPLOAD_BYTES.
   * 
   * This array is used to partition the buffer into upload-sized chunks.
   * 
   * Operations that modify chunkSizes, and/or add or remove data from pendingEventBuffer,
   * are synchronized on chunkSizes.
   */
  private final ChunkSizeList chunkSizes = new ChunkSizeList();
  
  /**
   * Total number of bytes written to pendingEventBuffer (and added to chunkSizes). Synchronized
   * on chunkSizes.
   */
  private long totalBytesWritten = 0;
  
  /**
   * True if we've discarded at least one event because we reached pendingEventBuffer
   * is full. Reset whenever an upload completes (thus freeing up memory). This ensures
   * that we don't "stutter" at the edge of the memory boundary, yielding a
   * confusing situation where events are dropped intermittently.
   */
  private boolean pendingEventsReachedLimit = false;
  
  /**
   * Incremented each time pendingEventsReachedLimit transitions from false to true.
   */
  private volatile int pendingEventsLimitCounter = 0;
  
  /**
   * Used to rate-limit buffer overflow warnings.
   */
  private static LogLimiter memoryWarnLimiter = new LogLimiter();
  
  /**
   * True if we have an in-flight upload request to the server.
   */
  private boolean uploadInProgress = false;
  
  /**
   * Timer used to generate upload events. Allocated when the first event is recorded.
   */
  private Timer uploadTimer = null;
  private TimerTask uploadTask = null;
  
  /**
   * Time when we last initiated an event batch upload, or null if we have not yet
   * started one.
   */
  private Long lastUploadStartMs = ScalyrUtil.currentTimeMillis();
  
  /**
   * Object used to synchronize access to pendingEventBuffer, pendingEventsReachedLimit,
   * and uploadInProgress.
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
  
  volatile double minUploadIntervalMs = TuningConstants.MIN_EVENT_UPLOAD_SPACING_MS;
  
  /**
   * EventFilter in effect for this uploader, or null if none.
   */
  volatile EventFilter eventFilter;
  
  /**
   * Construct an EventUploader to buffer events and upload them to the given LogService instance.
   */
  EventUploader(LogService logService, int memoryLimit, String sessionId, boolean autoUpload,
      EventAttributes serverAttributes) {
    this.logService = logService;
    this.autoUpload = autoUpload;
    
    this.memoryLimit = memoryLimit;
    pendingEventBuffer = new CircularByteArray(memoryLimit);
    
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
   * 
   * (NOTE: this is not foolproof. If the server request fails, or an upload was already in progress
   * when we are called, some events may not be uploaded. This method is only used in tests.)
   */
  synchronized void flush() {
    long bytesWrittenPriorToFlush;
    synchronized (chunkSizes) {
      bytesWrittenPriorToFlush = totalBytesWritten;
    }
    
    // Upload a chunk at a time until all data prior to our invocation is gone. Sleep briefly
    // between invocations, in part to avoid frantic spinning if an upload is already in progress
    // (in which case uploadTimerTick returns immediately).
    long sleepMs = 100;
    while (true) {
      synchronized (chunkSizes) {
        long bytesWrittenSinceFlush = totalBytesWritten - bytesWrittenPriorToFlush;
        if (pendingEventBuffer.numBufferedBytes() <= bytesWrittenSinceFlush) {
          break;
        }
      }
      
      uploadTimerTick(true);
      
      try {
        Thread.sleep(sleepMs);
        sleepMs = Math.min(sleepMs * 2, 10000);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
  
  /**
   * This method is called periodically by a timer. If it's been long enough since we last
   * sent a batch of events to the server, we snapshot the events currently buffered (or
   * a portion thereof, if there are too many to upload all at once) and initiate an upload.
   */
  synchronized void uploadTimerTick(boolean bypassWaitTimers) {
    final int bufferedBytes;
    RawJson eventsToUpload;
    
    synchronized (uploadSynchronizer) {
      bufferedBytes = timeToUpload(bypassWaitTimers);
      if (bufferedBytes < 0)
        return;
      
      eventsToUpload = new RawJson(){
        @Override public void writeJSONBytes(OutputStream out) throws IOException {
          out.write('[');
          
          // We subtract 1 here to eliminate the trailing comma after the last buffered event.
          pendingEventBuffer.writeOldestBytes(out, bufferedBytes - 1);
          
          out.write(']');
        }};
      
      uploadInProgress = true;
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
    sessionInfo.put("session", sessionId);
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
      lastUploadStartMs = ScalyrUtil.currentTimeMillis();
      String rawResponse = logService.uploadEvents(sessionId, sessionInfo, eventsToUpload, threadInfos);
      try {
        JSONObject parsedResponse = (JSONObject) new JSONParser().parse(rawResponse);
        
        // Adjust our upload interval based on the success or failure of this upload request.
        Object rawStatus = parsedResponse.get("status");
        String status = (rawStatus instanceof String) ? (String)rawStatus : "error/server";
        if (status.startsWith("success")) {
          minUploadIntervalMs *= TuningConstants.UPLOAD_SPACING_FACTOR_ON_SUCCESS;
          minUploadIntervalMs = Math.max(minUploadIntervalMs, TuningConstants.MIN_EVENT_UPLOAD_SPACING_MS);
          
          synchronized (uploadSynchronizer) {
            synchronized (chunkSizes) {
              ScalyrUtil.Assert(chunkSizes.getFirst() == bufferedBytes,
                  "event buffer chunk was resized while being uploaded");
              chunkSizes.removeFirst();
            }
            
            pendingEventBuffer.discardOldestBytes(bufferedBytes);
            pendingEventsReachedLimit = false;
          }
        } else {
          // Note that we back off for all errors, not just error/server/backoff. Other errors are liable to
          // be systemic, and there's little reason to retry an upload frequently in the face of systemic errors.
          minUploadIntervalMs *= TuningConstants.UPLOAD_SPACING_FACTOR_ON_BACKOFF;
          minUploadIntervalMs = Math.min(minUploadIntervalMs, TuningConstants.MAX_EVENT_UPLOAD_SPACING_MS);
        }
      } catch (ParseException ex) {
        // This shouldn't occur, as the underlying service framework verifies that the server's
        // response is valid JSON.
        throw new RuntimeException(ex);
      }
    } finally {
      synchronized (uploadSynchronizer) {
        uploadInProgress = false;
      }
    }
  }
  
  /**
   * Return if it's time to initiate a new upload batch, return the number of event payload bytes
   * (from pendingEventBuffer) to upload. Otherwise return -1.
   * 
   * Caller must hold the locks on "this" and uploadSynchronizer.
   */
  private int timeToUpload(boolean bypassWaitTimers) {
    // If we have an upload request in flight, don't initiate another one now. 
    if (uploadInProgress)
      return -1;
    
    long nowMs = ScalyrUtil.currentTimeMillis();
    
    // Enforce a minimum start-to-start spacing between uploads, to avoid overloading the server.
    if (!bypassWaitTimers && lastUploadStartMs != null) {
      long msSinceLastUpload = nowMs - lastUploadStartMs;
      if (msSinceLastUpload < minUploadIntervalMs)
        return -1;
    }
    
    // Get the size of the chunk to upload.
    synchronized (chunkSizes) {
      int bufferedBytes = chunkSizes.getFirst();
      if (bufferedBytes == 0) {
        // nothing to upload
        pendingEventsReachedLimit = false;
        return -1;
      }
      
      // Wait until the buffer is reasonably full, or it has been a fair while since we last initiated
      // an upload.
      boolean bufferFairlyFull = (bufferedBytes > _eventUploadByteThreshold);
      boolean itsBeenAWhile = (lastUploadStartMs == null
          || nowMs - lastUploadStartMs >= TuningConstants.EVENT_UPLOAD_TIME_THRESHOLD_MS);
      
      if (bypassWaitTimers || bufferFairlyFull || itsBeenAWhile) {
        // Prevent further data from being added to this chunk.
        chunkSizes.closeFirst();
        
        return bufferedBytes;
      } else {
        return -1;
      }
    }
  }
  
  /**
   * If true, then normal timer-based event uploading is disabled. For use only in tests.
   * Should not be used by client applications (this means you!). 
   */
  public static volatile boolean _disableUploadTimer = false;
  
  private synchronized void launchUploadTimer() {
    if (uploadTimer == null) {
      ourHostname = ScalyrUtil.getHostname();
      ourIpAddress = ScalyrUtil.getIpAddress();
      
      uploadTimer = new Timer("EventUploader", true);
      if (autoUpload) {
        uploadTask = new TimerTask(){
          @Override public void run() {
            try {
              if (!_disableUploadTimer)
                uploadTimerTick(false);
            } catch (Throwable ex) {
              Logging.log(Severity.warning, Logging.tagInternalError, "Exception in Logs upload timer", ex);
            }
          }};
        uploadTimer.schedule(uploadTask, TuningConstants.EVENT_UPLOAD_CHECK_INTERVAL,
            TuningConstants.EVENT_UPLOAD_CHECK_INTERVAL);
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
     * Number of start events in this thread that haven't yet been balanced by an end event.
     * Includes events which are discarded by a filter or due to buffer overflow. Does not
     * include the implicit session or thread spans.
     */
    private int spanNesting;
    
    /**
     * Number of start events in this thread that must be balanced by an end event, to reach
     * a point where no enclosing start event was discarded by a filter. In other words, this
     * is equal to spanNesting, minus the contiguous sequence of outermost non-discarded spans.
     * 
     * This value does not reflect events which were discarded due to the event buffer filling
     * up (see bufferLimitDiscardSpanNesting).
     */
    private int filterDiscardSpanNesting;
    
    /**
     * Like filterDiscardSpanNesting, but reflects discards due to buffer overflow rather than
     * filters.
     * 
     * Buffer overflow checks occur after event filtering, so if an event is discarded by a filter,
     * we don't attempt to place it in the buffer and it won't be reflected here.
     */
    private int bufferLimitDiscardSpanNesting;
    
    /**
     * Holds the value of pendingEventsLimitCounter when we last discarded an event in this thread,
     * or -1 if we have never done so.
     */
    private int eventDiscardGeneration = -1;
    
    PerThreadState(long threadId, String name) {
      this.threadId = threadId;
      this.name = name;
    }
    
    /**
     * Add a span-start event to the buffer, and return its timestamp.
     */
    Span start(Severity severity, EventAttributes attributes) {
      long timestampNs = getMonotonicNanos();
      
      ConvertAndAddResult result = convertAndAddToBuffer(timestampNs, LogService.SPAN_TYPE_START, severity, attributes, null,
          memoryLimit * TuningConstants.EVENT_BUFFER_RESERVED_PERCENT / 100, false);
      
      switch (result) {
      case discardedByFilter:
        filterDiscardSpanNesting++;
        break;
      
      case discardedByEventOverflow:
        bufferLimitDiscardSpanNesting++;
        break;
        
      case success:
        break;
      }
      
      if (result != ConvertAndAddResult.discardedByFilter) {
        if (filterDiscardSpanNesting > 0) {
          filterDiscardSpanNesting++;
        }
      }
      
      if (result != ConvertAndAddResult.discardedByEventOverflow) {
        if (bufferLimitDiscardSpanNesting > 0) {
          bufferLimitDiscardSpanNesting++;
        }
      }
      
      spanNesting++;
      
      return new Span(timestampNs, severity);
    }
    
    /**
     * Add an end-span event to the buffer.
     */
    void end(Span span, EventAttributes attributes) {
      long timestampNs = getMonotonicNanos();
      
      convertAndAddToBuffer(timestampNs, LogService.SPAN_TYPE_END, span.severity, attributes, span.startTime,
          memoryLimit * TuningConstants.EVENT_BUFFER_END_EVENT_RESERVED_PERCENT / 100, false);
      
      if (spanNesting > 0) {
        spanNesting--;
      } else {
        Logging.log(Severity.warning, Logging.tagMismatchedEnd, "Events.end(): no span is currently open in this thread."); 
      }
      
      if (filterDiscardSpanNesting > 0)
        filterDiscardSpanNesting--;
      
      if (bufferLimitDiscardSpanNesting > 0)
        bufferLimitDiscardSpanNesting--;
    }

    /**
     * Add a non-span event to the buffer.
     */
    void event(Severity severity, EventAttributes attributes) {
      event(severity, attributes, getMonotonicNanos());
    }
    
    /**
     * Add a non-span event to the buffer, with an explicitly specified timestamp.
     */
    void event(Severity severity, EventAttributes attributes, long timestampNs) {
      convertAndAddToBuffer(timestampNs, LogService.SPAN_TYPE_LEAF, severity, attributes, null,
          memoryLimit * TuningConstants.EVENT_BUFFER_RESERVED_PERCENT / 100, false);
    }
    
    /**
     * Run the given event through the EventFilter, convert it to JSON, and add it to our event buffer.
     * Return false if the event is discarded by a filter.
     * 
     * Don't record the event if it would leave less than reservedBufferSpace bytes remaining in
     * the buffer.
     * 
     * If isOverflowMessage is true, then this is a buffer-overflow message, which bypasses filters
     * and certain other checks.
     */
    private ConvertAndAddResult convertAndAddToBuffer(long timestamp, int spanType, Severity severity,
        EventAttributes attributes, Long startTs, int reservedBufferSpace, boolean isOverflowMessage) {
      EventFilter localFilter = eventFilter;
      if (localFilter != null && !isOverflowMessage) {
        FilterInput filterInput = new FilterInput();
        filterInput.threadId = Long.toString(threadId);
        filterInput.timestampNs = timestamp;
        filterInput.spanType = spanType;
        filterInput.severity = severity;
        filterInput.attributes = attributes != null ? attributes : emptyAttributes;
        filterInput.isTopLevel = (spanNesting == 0);
        filterInput.inDiscardedSpan = (filterDiscardSpanNesting > 0);
      
        FilterOutput filterOutput = new FilterOutput(filterInput);
        localFilter.filter(filterInput, filterOutput);
        
        if (filterOutput.discardEvent)
          return ConvertAndAddResult.discardedByFilter;
        
        if (bufferLimitDiscardSpanNesting > 0)
          return ConvertAndAddResult.discardedByEventOverflow;
        
        severity = filterOutput.severity;
        if (filterOutput.attributes != null)
          attributes = filterOutput.attributes;
      }
      
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
      
      if (startTs != null)
        json.put("startTS", startTs);
      
      return addEventToBuffer(json, spanType == LogService.SPAN_TYPE_END, reservedBufferSpace, isOverflowMessage);
    }
    
    private ConvertAndAddResult addEventToBuffer(JSONObject eventJson, boolean isEndEvent, int reservedBufferSpace,
        boolean isOverflowMessage) {
      boolean discardingDueToMemoryLimit;
      synchronized (uploadSynchronizer) {
        if (pendingEventsReachedLimit && !isEndEvent && !isOverflowMessage) {
          // If pendingEventsReachedLimit is true, we discard all events, to avoid
          // "stuttering" (letting in some events and not others) at the memory boundary.
          // However, we still let end events through, to maintain the integrity of span
          // nesting.
          discardingDueToMemoryLimit = true;
        } else if (bufferLimitDiscardSpanNesting > 0 && !isOverflowMessage) {
          // Don't upload any event that occurs inside a discarded span, including the
          // end event.
          discardingDueToMemoryLimit = true;
        } else {
          ByteArrayOutputStream stream = new ByteArrayOutputStream();
          try {
            eventJson.writeJSONBytes(stream);
          } catch (IOException ex) {
            // This should never happen, since we're working with in-memory data.
            throw new RuntimeException(ex);
          }
          stream.write(',');
          byte[] serialized = stream.toByteArray();
          int serializedLen = serialized.length;
          
          synchronized (chunkSizes) {
            if (pendingEventBuffer.append(serialized, 0, serializedLen, reservedBufferSpace)) {
              chunkSizes.append(serializedLen, _maxEventUploadBytes);
              totalBytesWritten += serializedLen;
              discardingDueToMemoryLimit = false;
            } else {
              discardingDueToMemoryLimit = true;
              pendingEventsReachedLimit = true;
              pendingEventsLimitCounter++;
            }
          }
        }
      }
      
      if (discardingDueToMemoryLimit && !isOverflowMessage) {
        int snapshot = pendingEventsLimitCounter;
        if (eventDiscardGeneration != snapshot) {
          eventDiscardGeneration = snapshot;
          
          // Record a log event noting that we had to discard some events.
          convertAndAddToBuffer(getMonotonicNanos(), LogService.SPAN_TYPE_LEAF, Severity.warning,
              new EventAttributes("tag", "eventBufferOverflow", "message", "Discarding log records due to buffer overflow"),
              null, 0, true);
        }
        
        if (memoryWarnLimiter.allow(TuningConstants.EVENT_UPLOAD_MEMORY_WARNING_INTERVAL_MS)) {
          Logging.log(Severity.warning, Logging.tagLogBufferOverflow,
              "com.scalyr.api.logs: Discarding event, as buffer size of "
              + memoryLimit + " bytes has been reached.");
        }
        
        return ConvertAndAddResult.discardedByEventOverflow;
      } else {
        return ConvertAndAddResult.success;
      }
    }
  }
  
  /**
   * See TuningConstants.MAX_EVENT_UPLOAD_BYTES. Sometimes modified in tests. Should not be used
   * by client applications (this means you!). 
   */
  public static volatile int _maxEventUploadBytes = TuningConstants.MAX_EVENT_UPLOAD_BYTES;
  
  /**
   * See TuningConstants.EVENT_UPLOAD_BYTE_THRESHOLD. Sometimes modified in tests. Should not be used
   * by client applications (this means you!). 
   */
  public static volatile int _eventUploadByteThreshold = TuningConstants.EVENT_UPLOAD_BYTE_THRESHOLD;
  
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
  
  enum ConvertAndAddResult {
    success,
    discardedByFilter,
    discardedByEventOverflow
  }
}
