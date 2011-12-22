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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

/**
 * Miscellaneous utility methods.
 * 
 * WARNING: this class, and all classes in the .internal package, should not be
 * used by client code. (This means you.) We reserve the right to make incompatible
 * changes to the .internal package at any time.
 */
public class ScalyrUtil {
  /**
   * Equivalent to a.equals(b), but handles the case where a and/or b are null.
   */
  public static boolean equals(Object a, Object b) {
    return (a == null) ? (b == null) : a.equals(b);
  }
  
  /**
   * Return all remaining content in the reader.
   */
  public static String readToEnd(Reader reader) throws IOException {
    StringBuilder sb = new StringBuilder();
    char[] buffer = new char[4096];
    while (true) {
      int count = reader.read(buffer, 0, buffer.length);
      if (count <= 0)
        break;
      sb.append(buffer, 0, count);
    }
    return sb.toString();
  }
  
  /**
   * Close the given stream, swallowing any exception.
   */
  public static void closeQuietly(InputStream stream) {
    try {
      if (stream != null) {
        stream.close();
      }
    } catch (IOException ex) {
    }
  }
  
  public static String readFileContent(File file) throws UnsupportedEncodingException, IOException {
    FileInputStream stream = new FileInputStream(file);
    try {
      return ScalyrUtil.readToEnd(new BufferedReader(new InputStreamReader(stream, "UTF-8")));
    } finally {
      ScalyrUtil.closeQuietly(stream);
    }
  }
  
  /**
   * (Near-)atomically create or overwrite the specified file with the specified content,
   * encoded as UTF-8.
   */
  public static void writeStringToFile(String text, File file) {
    try {
      // To ensure atomicity, we write to a side file and then rename into place.
      File tempFile = File.createTempFile(file.getName(), ".tmp", file.getParentFile());
      
      FileOutputStream output = new FileOutputStream(tempFile, false);
      OutputStreamWriter writer = new OutputStreamWriter(output);
      writer.write(text);
      writer.flush();
      writer.close();
      
      if (file.exists())
        file.delete();
      tempFile.renameTo(file);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
