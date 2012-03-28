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

package com.scalyr.api.knobs.util;

import java.util.HashSet;
import java.util.Set;

import com.scalyr.api.Callback;
import com.scalyr.api.knobs.Knob;

/**
 * A Whitelist provides a quick and efficient way to test whether a given string appears
 * in a comma-delimited list of strings stored in a Knob.
 */
public class Whitelist {
  private final Knob.String whitelistKnob;
  
  /**
   * Holds the result of parsing the most recent knob value. Null until first use.
   * Synchronize access on the Whitelist instance.
   */
  private Set<String> whitelistSet = null;
  
  /**
   * Construct a Whitelist based on the given Knob. The Knob should contain a comma-
   * delimited list of strings. An empty or missing Knob is treated as an empty list.
   * Leading or trailing whitespace, and whitespace adjacent to commas, is ignored.
   */
  public Whitelist(Knob.String whitelistKnob) {
    this.whitelistKnob = whitelistKnob;
  }
  
  /**
   * Return true if the given string appears in the whitelist.
   */
  public synchronized boolean isInWhitelist(String candidate) {
    if (whitelistSet == null) {
      whitelistSet = new HashSet<String>();
      whitelistKnob.addUpdateListener(new Callback<Knob>(){
        @Override public void run(Knob value) {
          rebuildStringSet();
        }});
      rebuildStringSet();
    }
   
    return whitelistSet.contains(candidate);
  }
  
  /**
   * Overwrite whitelistSet with the latest values from the Knob.
   */
  private synchronized void rebuildStringSet() {
    whitelistSet.clear();
    String knobValue = whitelistKnob.get();
    if (knobValue != null)
      for (String value : knobValue.split(","))
        whitelistSet.add(value.trim());
  }  

}
