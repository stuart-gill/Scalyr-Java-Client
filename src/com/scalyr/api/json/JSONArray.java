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
import java.util.ArrayList;
import java.util.List;

import com.scalyr.api.internal.FlushlessOutputStream;

/**
 * A JSON array. JSONObject supports java.util.List interface.
 * 
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public class JSONArray extends ArrayList<Object> implements List<Object>, JSONStreamAware {
  private static final long serialVersionUID = 3957988303675231981L;

  @Override public void writeJSONBytes(OutputStream out) throws IOException {
    out.write('[');
    
    boolean first = true;
    OutputStreamWriter writer = new OutputStreamWriter(new FlushlessOutputStream(out));
    
    for (Object value : this) {
      if (first)
        first = false;
      else
        writer.write(',');

      if (value == null) {
        writer.write("null");
        continue;
      }

      writer.flush();
      if (value instanceof JSONStreamAware) {
        ((JSONStreamAware)value).writeJSONBytes(out);
      } else {
        JSONValue.writeJSONBytes(value, out);
      }
    }
    
    out.write(']');
  }
  
  @Override
  public String toString() {
    return JSONValue.toJSONString(this);
  }

}
