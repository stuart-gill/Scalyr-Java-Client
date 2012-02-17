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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EventAttributes {
  final Map<String, Object> values = new HashMap<String, Object>();
  
  public EventAttributes() {
  }
  
  public EventAttributes(String key, Object value) {
    put(key, value);
  }
  
  public EventAttributes(String key1, Object value1, String key2, Object value2) {
    put(key1, value1);
    put(key2, value2);
  }
  
  public EventAttributes(String key, Object value, String key2, Object value2, String key3, Object value3) {
    put(key, value);
    put(key2, value2);
    put(key3, value3);
  }
  
  public EventAttributes(String key, Object value, String key2, Object value2, String key3, Object value3,
      String key4, Object value4) {
    put(key, value);
    put(key2, value2);
    put(key3, value3);
    put(key4, value4);
  }
  
  public EventAttributes(String key, Object value, String key2, Object value2, String key3, Object value3,
      String key4, Object value4, String key5, Object value5) {
    put(key, value);
    put(key2, value2);
    put(key3, value3);
    put(key4, value4);
    put(key5, value5);
  }
  
  public EventAttributes(Object[] inputs) {
    for (int i = 0; i < inputs.length; i += 2)
      put((String)inputs[i], inputs[i+1]);
  }
  
  public EventAttributes(EventAttributes objectToCopy) {
    for (Map.Entry<String, Object> entry : objectToCopy.values.entrySet())
      put(entry.getKey(), entry.getValue());
  }
  
  public Object get(String key) {
    return values.get(key);
  }
  
  public EventAttributes put(String key, Object value) {
    values.put(key, toValueType(value));
    return this;
  }
  
  public boolean containsKey(String key) {
    return values.containsKey(key);
  }
  
  /**
   * Convert the given value to one of the value types we can store in an event property.
   */
  private static Object toValueType(Object value) {
    if (value instanceof String)
      return value;
    else if (value instanceof Byte)
      return (int)(Byte)value;
    else if (value instanceof Short)
      return (int)(Short)value;
    else if (value instanceof Integer)
      return value;
    else if (value instanceof Long)
      return value;
    else if (value instanceof Float)
      return value;
    else if (value instanceof Double)
      return value;
    else if (value instanceof Boolean)
      return value;
    else if (value instanceof Date)
      return ((Date) value).getTime();
    else if (value == null)
      return value;
    else
      return value.toString();
  }
  
  @Override public String toString() {
    return values.toString();
  }
}
