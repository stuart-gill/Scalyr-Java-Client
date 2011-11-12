/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api.json;

public interface JSONAware {
  /**
   * @return JSON text
   */
  String toJSONString();
}
