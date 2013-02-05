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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.scalyr.api.internal.ScalyrUtil;


/**
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public class JSONValue {
    /**
     * Encode an object into JSON text and write it to out.
     * <p>
     * If this object is a Map or a List, and it's also a JSONStreamAware, JSONStreamAware will be considered firstly.
     * <p>
     * DO NOT call this method from writeJSONString(Writer) of a class that implements both JSONStreamAware and (Map or List) with 
     * "this" as the first parameter, use JSONObject.writeJSONString(Map, Writer) or JSONArray.writeJSONString(List, Writer) instead. 
     * 
     * @param value
     * @param writer
     */
    public static void writeJSONBytes(Object value, OutputStream out) throws IOException {
    if (value == null) {
      writeUTF8("null", out);
      return;
    }

    if (value instanceof String) {
      out.write('\"');
      writeUTF8(escape((String) value), out);
      out.write('\"');
      return;
    }

    if (value instanceof Double) {
      if (((Double) value).isInfinite() || ((Double) value).isNaN())
        writeUTF8("null", out);
      else
        writeUTF8(value.toString(), out);
      return;
    }

    if (value instanceof Float) {
      if (((Float) value).isInfinite() || ((Float) value).isNaN())
        writeUTF8("null", out);
      else
        writeUTF8(value.toString(), out);
      return;
    }

    if (value instanceof Number) {
      writeUTF8(value.toString(), out);
      return;
    }

    if (value instanceof Boolean) {
      writeUTF8(value.toString(), out);
      return;
    }

    if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      out.write('`');
      out.write('b');
      new DataOutputStream(out).writeInt(bytes.length);
      out.write(bytes);
      return;
    }

    if ((value instanceof JSONStreamAware)) {
      ((JSONStreamAware) value).writeJSONBytes(out);
      return;
    }

    writeUTF8(value.toString(), out);
    }
    
    private static void writeUTF8(String text, OutputStream out) throws IOException {
      out.write(text.getBytes(ScalyrUtil.utf8));
    }
  
        /**
         * Convert an object to JSON text.
         * <p>
         * If this object is a Map or a List, and it's also a JSONAware, JSONAware will be considered firstly.
         * <p>
         * DO NOT call this method from toJSONString() of a class that implements both JSONAware and Map or List with 
         * "this" as the parameter, use JSONObject.toJSONString(Map) or JSONArray.toJSONString(List) instead. 
         * 
         * @param value
         * @return JSON text, or "null" if value is null or it's an NaN or an INF number.
         */
        public static String toJSONString(Object value){
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          try {
            writeJSONBytes(value, buffer);
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
          return new String(buffer.toByteArray(), ScalyrUtil.utf8);
        }

        /**
         * Escape quotes, \, /, \r, \n, \b, \f, \t and other control characters (U+0000 through U+001F).
         * @param s
         * @return
         */
        public static String escape(String s){
                if(s==null)
                        return null;
        StringBuffer sb = new StringBuffer();
        escape(s, sb);
        return sb.toString();
    }

    /**
     * @param s - Must not be null.
     * @param sb
     */
    static void escape(String s, StringBuffer sb) {
                for(int i=0;i<s.length();i++){
                        char ch=s.charAt(i);
                        switch(ch){
                        case '"':
                                sb.append("\\\"");
                                break;
                        case '\\':
                                sb.append("\\\\");
                                break;
                        case '\b':
                                sb.append("\\b");
                                break;
                        case '\f':
                                sb.append("\\f");
                                break;
                        case '\n':
                                sb.append("\\n");
                                break;
                        case '\r':
                                sb.append("\\r");
                                break;
                        case '\t':
                                sb.append("\\t");
                                break;
                        case '/':
                                sb.append("\\/");
                                break;
                        default:
                //Reference: http://www.unicode.org/versions/Unicode5.1.0/
                                if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
                                        String ss=Integer.toHexString(ch);
                                        sb.append("\\u");
                                        for(int k=0;k<4-ss.length();k++){
                                                sb.append('0');
                                        }
                                        sb.append(ss.toUpperCase());
                                }
                                else{
                                        sb.append(ch);
                                }
                        }
                }//for
        }

}