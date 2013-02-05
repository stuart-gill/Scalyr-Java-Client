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
 * 
 * Taken from the json-simple library, by Yidong Fang and Chris Nokleberg.
 * This copy has been modified by Scalyr, Inc.; see README.txt for details.
 */

package com.scalyr.api.json;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import com.scalyr.api.internal.FlushlessOutputStream;

/**
 * A JSON object. Key value pairs are unordered. JSONObject supports
 * java.util.Map interface.
 * 
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public class JSONObject extends HashMap<String, Object> implements JSONStreamAware {
  public JSONObject() {
    super();
  }

  @Override public void writeJSONBytes(OutputStream out) throws IOException {
    out.write('{');

    boolean first = true;
    OutputStreamWriter writer = new OutputStreamWriter(new FlushlessOutputStream(out));
    
    for (Map.Entry<String, Object> entry : entrySet()) {
      if (first)
        first = false;
      else
        writer.write(',');
      
      writer.write('\"');
      writer.write(escape(String.valueOf(entry.getKey())));
      writer.write('\"');
      writer.write(':');
      writer.flush();
      
      if (entry.getValue() instanceof JSONStreamAware) {
        ((JSONStreamAware)entry.getValue()).writeJSONBytes(out);
      } else {
        JSONValue.writeJSONBytes(entry.getValue(), out);
      }
    }
    writer.flush();

    out.write('}');
  }

  @Override
  public String toString() {
    return JSONValue.toJSONString(this);
  }

  /**
   * Escape quotes, \, /, \r, \n, \b, \f, \t and other control characters
   * (U+0000 through U+001F). It's the same as JSONValue.escape() only for
   * compatibility here.
   * 
   * @param s
   * @return
   */
  public static String escape(String s) {
    return JSONValue.escape(s);
  }
}