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
 * Utilities for converting data from one type to another.
 * 
 * Used to avoid type confusion when reading fields from a JSON object.
 */
public class Converter {
  /**
   * Convert any numeric type to Integer.
   * <p>
   * A null input is returned as-is. Non-numeric inputs trigger an exception. Out-of-range
   * values trigger undefined behavior.
   */
  public static Integer toInteger(Object value) {
    if (value instanceof Integer)
      return (Integer)value;
    else if (value instanceof Long)
      return (int)(long)(Long)value;
    else if (value instanceof Double)
      return (int)(double)(Double)value;
    else if (value == null)
      return null;
    else
      throw new RuntimeException("Can't convert [" + value + "] to Integer");
  }
  
  /**
   * Convert any numeric type to Long.
   * <p>
   * A null input is returned as-is. Non-numeric inputs trigger an exception. Out-of-range
   * values trigger undefined behavior.
   */
  public static Long toLong(Object value) {
    if (value instanceof Integer)
      return (long)(int)(Integer)value;
    else if (value instanceof Long)
      return (Long)value;
    else if (value instanceof Double)
      return (long)(double)(Double)value;
    else if (value == null)
      return null;
    else
      throw new RuntimeException("Can't convert [" + value + "] to Long");
  }
  
  /**
   * Convert any numeric type to Double.
   * <p>
   * A null input is returned as-is. Non-numeric inputs trigger an exception.
   */
  public static Double toDouble(Object value) {
    if (value instanceof Integer)
      return (double)(int)(Integer)value;
    else if (value instanceof Long)
      return (double)(long)(Long)value;
    else if (value instanceof Double)
      return (Double)value;
    else if (value == null)
      return null;
    else
      throw new RuntimeException("Can't convert [" + value + "] to Double");
  }
  
  /**
   * Return the given value coerced to Boolean.
   * <p>
   * A null input is returned as-is. Non-Boolean inputs trigger an exception.
   */
  public static Boolean toBoolean(Object value) {
    if (value instanceof Boolean)
      return (Boolean) value;
    else if (value == null)
      return null;
    else
      throw new RuntimeException("Can't convert [" + value + "] to Boolean");
  }
  
  /**
   * Return the given value converted to a String.
   * <p>
   * A null input is returned as-is.
   */
  public static String toString(Object value) {
    if (value != null)
      return value.toString();
    else
      return null;
  }
}