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

package com.scalyr.api.internal;

/**
 * WARNING: this class, and all classes in the .internal package, should not be
 * used by client code. (This means you.) We reserve the right to make incompatible
 * changes to the .internal package at any time.
 */
public class Logging {
  public static void info(String message) {
    System.out.println(message);
  }
  
  public static void info(String message, Throwable ex) {
    System.out.println(message);
    ex.printStackTrace(System.out);
  }
  
  public static void warn(String message) {
    System.out.println(message);
  }
  
  public static void warn(String message, Throwable ex) {
    System.out.println(message);
    ex.printStackTrace(System.out);
  }
}
