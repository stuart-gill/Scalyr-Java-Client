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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * An EventAttributes object encapsulates named attributes to be associated with an event.
 * It can store Boolean, String, or numeric values. All other data types are converted to
 * String.
 */
public class EventAttributes {
  final Map<String, Object> values = new HashMap<String, Object>();
  
  /**
   * Construct an empty attribute list.
   */
  public EventAttributes() {
  }
  
  /**
   * Construct a one-entry attribute list.
   * 
   * @param key The attribute name. It is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   * 
   * @param value The attribute value.
   */
  public EventAttributes(String key, Object value) {
    put(key, value);
  }
  
  /**
   * Construct a two-entry attribute list.
   * 
   * @param key1 The attribute name. It is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   * 
   * @param value1 The attribute value.
   */
  public EventAttributes(String key1, Object value1, String key2, Object value2) {
    put(key1, value1);
    put(key2, value2);
  }
  
  /**
   * Construct a three-entry attribute list.
   * 
   * @param key1 The attribute name. It is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   * 
   * @param value1 The attribute value.
   */
  public EventAttributes(String key1, Object value1, String key2, Object value2, String key3, Object value3) {
    put(key1, value1);
    put(key2, value2);
    put(key3, value3);
  }
  
  /**
   * Construct a four-entry attribute list.
   * 
   * @param key1 The attribute name. It is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   * 
   * @param value1 The attribute value.
   */
  public EventAttributes(String key1, Object value1, String key2, Object value2, String key3, Object value3,
      String key4, Object value4) {
    put(key1, value1);
    put(key2, value2);
    put(key3, value3);
    put(key4, value4);
  }
  
  /**
   * Construct a five-entry attribute list.
   * 
   * @param key1 The attribute name. It is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   * 
   * @param value1 The attribute value.
   */
  public EventAttributes(String key1, Object value1, String key2, Object value2, String key3, Object value3,
      String key4, Object value4, String key5, Object value5) {
    put(key1, value1);
    put(key2, value2);
    put(key3, value3);
    put(key4, value4);
    put(key5, value5);
  }
  
  /**
   * Construct a six-entry attribute list. (For more than six attributes, use {@link #EventAttributes(Object[])},
   * or add attributes individually using {@link #put(String,Object)}.)
   * 
   * @param key1 The attribute name. It is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   * 
   * @param value1 The attribute value.
   */
  public EventAttributes(String key1, Object value1, String key2, Object value2, String key3, Object value3,
      String key4, Object value4, String key5, Object value5, String key6, Object value6) {
    put(key1, value1);
    put(key2, value2);
    put(key3, value3);
    put(key4, value4);
    put(key5, value5);
    put(key6, value6);
  }
  
  /**
   * Construct an attribute list with an arbitrary number of entries. Even-numbered array elements are
   * attribute names, and odd-numbered array elements are attribute values.
   * <p>
   * For attribute names, it is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   */
  public EventAttributes(Object[] inputs) {
    for (int i = 0; i < inputs.length; i += 2)
      put((String)inputs[i], inputs[i+1]);
  }
  
  /**
   * Construct a shallow copy of the given object.
   */
  public EventAttributes(EventAttributes source) {
    addAll(source);
  }
  
  /**
   * Copy all attributes from the given object to this object. In case of conflicts,
   * attributes from objectToCopy overwrite the existing attributes in this object.
   * Non-conflicting attributes in this object are retained.
   * 
   * Return this object.
   */
  public EventAttributes addAll(EventAttributes source) {
    for (Map.Entry<String, Object> entry : source.values.entrySet())
      put(entry.getKey(), entry.getValue());
    
    return this;
  }
  
  /**
   * Copy all attributes from the given object to this object. In case of conflicts,
   * attributes from objectToCopy are ignored.
   * 
   * Return this object.
   */
  public EventAttributes underwriteFrom(EventAttributes source) {
    for (Map.Entry<String, Object> entry : source.values.entrySet())
      if (!containsKey(entry.getKey()))
        put(entry.getKey(), entry.getValue());
    
    return this;
  }
  
  /**
   * Return the value of the specified attribute, or null if the attribute is not defined.
   */
  public Object get(String key) {
    return values.get(key);
  }
  
  /**
   * Store (or overwrite) a value for the specified attribute.
   * 
   * @param key The attribute name. It is best to use standard identifiers (letters, digits, and
   * underscores, not beginning with a digit), as these are easier to work with in the Scalyr Logs
   * user interface.
   * 
   * @param value The attribute value.
   */
  public EventAttributes put(String key, Object value) {
    values.put(key, toValueType(value));
    return this;
  }
  
  /**
   * Return true if a value is stored for the specified attribute.
   */
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
    // We return our attributes in alphabetical order, for consistency in tests.
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    
    List<String> keys = new ArrayList<String>(values.keySet());
    Collections.sort(keys);
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0)
        sb.append(", ");
      
      String key = keys.get(i);
      sb.append(key);
      sb.append('=');
      sb.append(values.get(key));
    }
    
    sb.append('}');
    return sb.toString();
  }
  
  /**
   * We implement equals() in a manner that respects MATCH_ANY -- see MATCH_ANY for details. We also treat Integers and
   * Longs as equivalent; this is handy in tests.
   */
  @Override public boolean equals(Object obj) {
    if (obj == this)
      return true;
    
    if (getClass() != obj.getClass())
      return false;
    
    EventAttributes other = (EventAttributes) obj;
    if (values.size() != other.values.size())
      return false;
    
    // We compare values one at a time, rather than invoking values.equals(), to implement MATCH_ANY and other
    // special features (see equivalentValues()).
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      Object otherValue = other.values.get(key);
      if (value == null) {
        if (otherValue != null || !other.values.containsKey(key)) {
          return false;
        }
      } else if (otherValue == null) {
        return false;
      } else {
        if (!equivalentValues(value, otherValue)) {
          return false;
        }
      }
    }
    
    return true;
  }
  
  /**
   * Return true if the two values are equal, or "equivalent". We treat MATCH_ANY as equivalent to any
   * non-null value, and we treat an Integer as equivalent to a Long with the same numeric value.
   * 
   * We assume both parameters are non-null.
   */
  private static boolean equivalentValues(Object a, Object b) {
    if (a.equals(b))
      return true;
    
    if (a == MATCH_ANY || b == MATCH_ANY)
      return true;
    
    if (a instanceof Integer && b instanceof Long) {
      return (int)(Integer)a == (long)(Long)b;
    }
    
    if (a instanceof Long && b instanceof Integer) {
      return (long)(Long)a == (int)(Integer)b;
    }
    
    return false;
  }
  
  /**
   * Special value which EventAttributes.equals() treats as matching any non-null value. Useful in tests.
   */
  public static String MATCH_ANY = "__MATCH_ANY_VALUE__";
  
  @Override public int hashCode() {
    return values.hashCode();
  }
  
  /**
   * Return all attribute names.
   */
  public Collection<String> getNames() {
    return values.keySet();
  }
  
  /**
   * Return all attributes -- a collection of {attribute name, attribute value} pairs.
   */
  public Collection<Map.Entry<String, Object>> getEntries() {
    return values.entrySet();
  }
}
