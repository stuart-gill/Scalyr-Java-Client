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


/**
 * A counter which records its value to Scalyr Logs at regular intervals.
 */
public class CounterGauge extends Gauge {
  /**
   * The current value of the counter.
   */
  private final AtomicDouble value = new AtomicDouble();
  
  /**
   * Create a CounterGauge, and register it under the specified attributes.
   */
  public CounterGauge(EventAttributes attributes) {
    this(attributes, 0);
  }
  
  /**
   * Create a CounterGauge with the specified value, and register it under the specified attributes.
   */
  public CounterGauge(EventAttributes attributes, double initialValue) {
    value.set(initialValue);
    Gauge.register(this, attributes);
  }
  
  /**
   * Return the current counter value.
   */
  public double getValue() {
    return value.get();
  }
  
  /**
   * Set the current counter value, overwriting the previous value. To increment the value,
   * call increment().
   */
  public void setValue(double x) {
    value.set(x);
  }
  
  /**
   * Increment the counter by one.
   */
  public void increment() {
    increment(1.0);
  }
  
  /**
   * Increment the counter by the specified delta.
   */
  public void increment(double delta) {
    value.add(delta);
  }
  
  /**
   * Implementation of Gauge.sample().
   */
  @Override public Object sample() {
    return value.get();
  }

  /**
   * All counters created by increment(EventAttributes, double).
   */
  private static Map<EventAttributes, CounterGauge> counterTable = new HashMap<EventAttributes, CounterGauge>();
  
  /**
   * Create a CounterGauge with the specified attributes. If this method (or setValue) has been called
   * previously with the same attributes, we re-use the same CounterGauge. Either way, we then increment the
   * counter by the specified delta.
   * <p>
   * This method allows you to record counters associated with variable attributes, without having
   * to explicitly maintain a set of CounterGauges. You should be careful to limit the number of
   * distinct attribute sets used, as each unique CounterGauge imposes a certain resource cost
   * (primarily the CPU and bandwidth cost of periodically logging its value). Beyond a few hundred
   * Gauges, you should start to keep an eye on the cost.
   */
  public static void increment(EventAttributes attributes, double delta) {
    CounterGauge counter;
    
    synchronized (counterTable) {
      counter = counterTable.get(attributes);
      if (counter == null) {
        counter = new CounterGauge(attributes);
        counterTable.put(attributes, counter);
      }
    }
    
    counter.increment(delta);
  }
  
  /**
   * Create a CounterGauge with the specified attributes. If this method (or increment) has been called
   * previously with the same attributes, we re-use the same CounterGauge. Either way, we then set the
   * counter to the specified value.
   * <p>
   * This method allows you to record values associated with variable attributes, without having
   * to explicitly maintain a set of CounterGauges. You should be careful to limit the number of
   * distinct attribute sets used, as each unique CounterGauge imposes a certain resource cost
   * (primarily the CPU and bandwidth cost of periodically logging its value). Beyond a few hundred
   * Gauges, you should start to keep an eye on the cost.
   */
  public static void setStaticValue(EventAttributes attributes, double value) {
    CounterGauge counter;
    
    synchronized (counterTable) {
      counter = counterTable.get(attributes);
      if (counter == null) {
        counter = new CounterGauge(attributes);
        counterTable.put(attributes, counter);
      }
    }
    
    counter.setValue(value);
  }
}
