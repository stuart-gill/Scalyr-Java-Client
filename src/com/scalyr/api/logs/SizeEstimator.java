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

import com.scalyr.api.json.JSONObject;

/**
 * Utilities for estimating the heap usage of various Java objects. Used to bound
 * the memory usage of the Logs client.
 * <p>
 * TODO: eliminate this class: it is fragile and nonportable, and doesn't account for 32-bit
 * vs. 64-bit JVMs. We will most likely replace JSON with a binary format for log buffering
 * and upload, at which point SizeEstimator will no longer be needed.
 */
public class SizeEstimator {
  /**
   * Estimated overhead per Java heap object.
   */
  public static final long OBJECT_HEAP_OVERHEAD = 8;
  
  /**
   * Estimated size of a zero-length ArrayList.
   */
  public static final long ARRAYLIST_OVERHEAD = OBJECT_HEAP_OVERHEAD * 2 + 20;

  public static long estimateSize(JSONObject object) {
    long baseObject = OBJECT_HEAP_OVERHEAD + 20;
    long entriesArray = OBJECT_HEAP_OVERHEAD + object.size() * 8; // assuming 50% hash table utilization
    long entryObjects = (OBJECT_HEAP_OVERHEAD + 8) * object.size();
      
    // Note that we ignore space used by hash table keys, on the assumption that they will
    // generally be interned string literals.
    int valueSize = 0;
    for (Object value : object.values()) {
      if (value == null)
        ;
      else if (value instanceof String)
        valueSize += OBJECT_HEAP_OVERHEAD + 8 + ((String)value).length() * 2;
      else if (value instanceof JSONObject)
        valueSize += estimateSize((JSONObject)value);
      else
        valueSize += OBJECT_HEAP_OVERHEAD + 8;
    }
    
    return OBJECT_HEAP_OVERHEAD + baseObject + entriesArray + entryObjects + valueSize;
  }
}
