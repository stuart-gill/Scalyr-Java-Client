/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
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
