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
 * 
 * Taken from the json-simple library, by Yidong Fang and Chris Nokleberg.
 * This copy has been modified by Scalyr, Inc.; see README.txt for details.
 */

package com.scalyr.api.json;

public interface JSONAware {
  /**
   * @return JSON text
   */
  String toJSONString();
}
