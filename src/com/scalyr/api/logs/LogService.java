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

package com.scalyr.api.logs;

import com.scalyr.api.ScalyrException;
import com.scalyr.api.ScalyrNetworkException;
import com.scalyr.api.internal.ScalyrService;
import com.scalyr.api.json.JSONArray;
import com.scalyr.api.json.JSONObject;

/**
 * Encapsulates the raw HTTP-level API to the Logs service.
 */
public class LogService extends ScalyrService {
  /**
   * Construct a LogService.
   * 
   * @param apiToken The API authorization token to use when communicating with the server. (If you need
   *     to use multiple api tokens, construct a separate LogService instance for each.)
   */
  public LogService(String apiToken) {
    super(apiToken);
    
    setServerAddress("https://log.scalyr.com");
  }
  
  public @Override synchronized LogService setServerAddress(String value) {
    return (LogService) super.setServerAddress(value);
  }
  
  /**
   * Upload a batch of events to the Scalyr Logs service. See the
   * <a href="https://log.scalyr.com/logHttpApi">HTTP API documentation</a> for a detailed description
   * of each parameter.
   * 
   * @param sessionId ID of this process instance.
   * @param sessionInfo Attributes to associate with this session. Should be remain constant for
   *     all calls to uploadEvents with a given session ID.
   * @param events The events to upload.
   * @param threadInfos Optional; contains information for the threads referenced in the events array.
   * 
   * @return The response from the server. See <a href='https://www.scalyr.com/httpApi'>scalyr.com/httpApi</a>.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
   */
  public String uploadEvents(String sessionId, JSONObject sessionInfo,
      JSONArray events, JSONArray threadInfos)
      throws ScalyrException, ScalyrNetworkException {
    JSONObject parameters = new JSONObject();
    
    parameters.put("token", apiToken);
    parameters.put("session", sessionId);
    if (sessionInfo != null)
      parameters.put("sessionInfo", sessionInfo);
    parameters.put("events", events);
    if (threadInfos != null && threadInfos.size() > 0)
      parameters.put("threads", threadInfos);
    
    return invokeApi("addEvents", parameters);
  }
  
  static final int SPAN_TYPE_LEAF  = 0;
  static final int SPAN_TYPE_START = 1;
  static final int SPAN_TYPE_END   = 2;
}
