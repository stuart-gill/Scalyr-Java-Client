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

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.scalyr.api.TuningConstants;
import com.scalyr.api.internal.Logging;

/**
 * A Gauge is an object which can report a value on demand. Gauges are used to periodically
 * sample a value and record it in the Events log. Sample usage:
 * 
 * <pre>
 *   Gauge.register(new Gauge(){
 *     @Override public Object sample() {
 *       return someComputation();
 *   }}, new EventAttributes("tag", "foo"));
 * </pre>
 */
public abstract class Gauge {
  /**
   * Report the current value for this gauge.
   */
  public abstract Object sample();
  
  /**
   * Timer used to sample gauges events. Allocated when the first gauge is registered.
   */
  private static Timer sampleTimer = null;
  private static TimerTask sampleTask = null;
  
  /**
   * Holds an entry for each registered gauge.
   */
  private static Map<Gauge, EventAttributes> registeredGauges = new HashMap<Gauge, EventAttributes>(); 
  
  /**
   * Register a gauge. We will record the gauge's value once per minute, associating
   * the given attributes.
   */
  public static void register(Gauge gauge, EventAttributes attributes) {
    synchronized (registeredGauges) {
      registeredGauges.put(gauge, attributes);
      
      // If the timer task hasn't been launched yet, launch it now.
      if (sampleTimer == null) {
        sampleTimer = new Timer("SampleTimer", true);
        sampleTask = new TimerTask(){
          @Override public void run() {
            recordGaugeValues();
          }};
        sampleTimer.schedule(sampleTask, TuningConstants.GAUGE_SAMPLE_INTERVAL_MS,
            TuningConstants.GAUGE_SAMPLE_INTERVAL_MS);
      }
    }
  }
  
  private static void recordGaugeValues() {
    synchronized (registeredGauges) {
      for (Map.Entry<Gauge, EventAttributes> entry : registeredGauges.entrySet()) {
        try {
          Object value = entry.getKey().sample();
          if (value != null) {
            EventAttributes attributes = new EventAttributes(entry.getValue());
            attributes.put("value", value);
            Events.info(attributes);
          }
        } catch (Exception ex) {
          Logging.log(Severity.warning, Logging.tagGaugeThrewException,
              "Exception in Gauge [" + entry.getKey() + "] (attributes " + entry.getValue() + ")", ex);
        }
      }
    }
  }
  
  /**
   * Deregister a gauge. If we were recording the gauge's value, we cease doing so.
   */
  public static void deregister(Gauge gauge) {
    synchronized (registeredGauges) {
      registeredGauges.remove(gauge);
    }
  }
}
